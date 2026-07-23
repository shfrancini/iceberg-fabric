#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import base64
import csv
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from remote_transport import TransportConfigError, build_transport, shell_join

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = Path(__file__).resolve().with_name("validate_csv_against_reference.py")
REPORT_NAME = "unified_queue_pipeline"
CSV_FIELDS = ("seq_id", "timestamp", "tick", "adc_value", "wire_id", "tpc", "slot", "plane", "packet_file")


class PipelineError(RuntimeError):
    pass


@dataclass(frozen=True)
class Layout:
    run_id: str
    local_run_dir: Path
    report_dir: Path
    artifact_dir: Path
    viz_dir: Path
    local_decoded_chunk_dir: Path
    command_log: Path
    manifest_json: Path
    summary_json: Path
    dry_run_json: Path
    local_combined_csv: Path
    remote_receiver_run_dir: str
    remote_sender_run_dir: str
    remote_raw_chunk_dir: str
    remote_decoded_chunk_dir: str
    remote_combined_csv: str
    remote_manifest_json: str
    remote_summary_json: str
    remote_helper_dir: str
    remote_validator: str


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def slug(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip("-")
    return cleaned or "run"


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_config(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise PipelineError(f"config is not a mapping: {path}")
    return config


def parse_last_int(text: str) -> int:
    values = [int(line.strip()) for line in text.splitlines() if line.strip().isdigit()]
    if not values:
        raise PipelineError(f"no integer found in command output: {text!r}")
    return values[-1]


def parse_sentinel_json(stdout: str, sentinel: str) -> Any:
    for line in stdout.splitlines():
        if sentinel in line:
            return json.loads(line.split(sentinel, 1)[1])
    raise PipelineError(f"missing {sentinel} in command output")


def extract_json_object(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < start:
        raise ValueError("no JSON object found")
    return json.loads(text[start : end + 1])


def parse_export_stats(text: str) -> dict[str, Any]:
    marker = "Export complete:"
    summary = ""
    for line in text.splitlines():
        if line.startswith(marker):
            summary = line
    if not summary:
        return {"summary_found": False, "summary": ""}
    parts = summary.replace("Export complete: ", "").split(", ")
    try:
        return {
            "summary_found": True,
            "summary": summary,
            "packets_read": int(parts[0].split()[0]),
            "packets_parsed": int(parts[1].split()[0]),
            "packets_skipped": int(parts[2].split()[0]),
            "rows_written": int(parts[3].split()[0]),
        }
    except (IndexError, ValueError):
        return {"summary_found": False, "summary": summary}


def parse_tcpdump_stats(text: str) -> dict[str, Any]:
    stats: dict[str, Any] = {"summary_found": False}
    patterns = {
        "packets_captured": r"(\d+) packets captured",
        "packets_received_by_filter": r"(\d+) packets received by filter",
        "packets_dropped_by_kernel": r"(\d+) packets dropped by kernel",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, text)
        if match:
            stats[key] = int(match.group(1))
            stats["summary_found"] = True
    return stats


def load_csv_sample(path: Path, max_rows: int) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for index, row in enumerate(reader):
            if index >= max_rows:
                break
            rows.append(row)
    return rows


class PipelineRunner:
    def __init__(
        self,
        config: dict[str, Any],
        *,
        config_path: Path,
        source_mode: str | None = None,
        run_id: str | None = None,
        output_root: Path | None = None,
        adaptive: bool | None = None,
        validate: bool | None = None,
        max_live_chunks: int | None = None,
        follow: bool = False,
        timeout: int = 900,
    ):
        self.config = config
        self.config_path = config_path
        if source_mode:
            self.config["source_mode"] = source_mode
        if adaptive is not None:
            self.config.setdefault("decode", {}).setdefault("adaptive", {})["enabled"] = adaptive
        if validate is not None:
            self.config.setdefault("validation", {})["enabled"] = validate
        if max_live_chunks is not None:
            if self.config.get("source_mode") == "pcap_replay":
                chunk_packets = self.config.get("decode", {}).get("chunking", {}).get("chunk_packets")
                if chunk_packets in (None, ""):
                    raise PipelineError("--max-live-chunks cannot bound replay without decode.chunking.chunk_packets")
                replay = self.config.setdefault("pcap_replay", {})
                requested_limit = int(max_live_chunks) * int(chunk_packets)
                configured_limit = replay.get("packet_limit")
                if configured_limit not in (None, ""):
                    requested_limit = min(int(configured_limit), requested_limit)
                replay["packet_limit"] = requested_limit
                replay["max_replay_chunks"] = int(max_live_chunks)
                replay["packet_limit_source"] = "--max-live-chunks compatibility bound for replay smoke tests"
            else:
                self.config.setdefault("live_nic", {})["max_chunks"] = max_live_chunks
        self.timeout = timeout
        self.follow = follow
        self.layout = self.build_layout(run_id=run_id, output_root=output_root)
        self.transport = build_transport(
            self.config.get("transport"),
            slice_name=self.slice_name,
            log_file=self.layout.command_log,
        )
        self.chunks: list[dict[str, Any]] = []
        self.manifest: dict[str, Any] = self.base_manifest(status="created", phase="created")

    @property
    def source_mode(self) -> str:
        return str(self.config.get("source_mode", "")).strip()

    @property
    def label(self) -> str:
        return slug(str(self.config.get("label") or self.config.get("event_id") or self.source_mode))

    @property
    def slice_name(self) -> str:
        return str(self.config.get("slice", {}).get("name") or "")

    @property
    def sender_node(self) -> str:
        return str(self.config.get("slice", {}).get("sender_node") or "")

    @property
    def receiver_node(self) -> str:
        return str(self.config.get("slice", {}).get("receiver_node") or "")

    @property
    def receiver_interface(self) -> str:
        return str(self.config.get("network", {}).get("receiver_interface") or "")

    @property
    def sender_interface(self) -> str:
        return str(self.config.get("network", {}).get("sender_interface") or self.receiver_interface)

    @property
    def capture_filter(self) -> str:
        return str(self.config.get("network", {}).get("capture_filter") or "udp and dst port 2049")

    @property
    def receiver_root(self) -> str:
        return str(self.config.get("paths", {}).get("receiver_root") or "/home/ubuntu").rstrip("/")

    @property
    def sender_root(self) -> str:
        return str(self.config.get("paths", {}).get("sender_root") or "/home/ubuntu").rstrip("/")

    @property
    def wibviz_src(self) -> str:
        return str(self.config.get("paths", {}).get("wibviz_src") or "/home/ubuntu/wibviz_graph/src")

    @property
    def mapping_csv(self) -> str:
        return str(self.config.get("paths", {}).get("mapping_csv") or "")

    @property
    def chunk_packets(self) -> int | None:
        value = self.config.get("decode", {}).get("chunking", {}).get("chunk_packets")
        return None if value in (None, "") else int(value)

    @property
    def validation_enabled(self) -> bool:
        validation = self.config.get("validation", {})
        return bool(validation.get("enabled")) and bool(validation.get("reference_csv"))

    def build_layout(self, *, run_id: str | None, output_root: Path | None) -> Layout:
        root_value = output_root or Path(str(self.config.get("paths", {}).get("output_root") or "/tmp/wib_fabric_live_demo"))
        root = root_value.expanduser().resolve()
        final_run_id = run_id or f"{stamp()}-{self.label}-{self.source_mode}"
        local_run_dir = root / "runs" / final_run_id
        report_dir = local_run_dir / "reports"
        artifact_dir = local_run_dir / "artifacts"
        viz_dir = local_run_dir / "visualizations"
        local_decoded_chunk_dir = artifact_dir / "decoded_chunks"
        remote_receiver_run_dir = f"{self.receiver_root}/wibviz_runs/wib_fabric_live_demo/{final_run_id}"
        remote_helper_dir = f"{self.receiver_root}/wibviz_runs/wib_fabric_live_demo/helpers"
        remote_sender_run_dir = f"{self.sender_root}/wibviz_runs/wib_fabric_live_demo/{final_run_id}"
        remote_output_dir = f"{self.receiver_root}/wibviz_outputs/wib_fabric_live_demo/{final_run_id}"
        return Layout(
            run_id=final_run_id,
            local_run_dir=local_run_dir,
            report_dir=report_dir,
            artifact_dir=artifact_dir,
            viz_dir=viz_dir,
            local_decoded_chunk_dir=local_decoded_chunk_dir,
            command_log=report_dir / "commands.log",
            manifest_json=report_dir / "manifest.json",
            summary_json=report_dir / "summary.json",
            dry_run_json=report_dir / "dry_run.json",
            local_combined_csv=artifact_dir / "combined.csv",
            remote_receiver_run_dir=remote_receiver_run_dir,
            remote_sender_run_dir=remote_sender_run_dir,
            remote_raw_chunk_dir=f"{remote_output_dir}/raw_chunks",
            remote_decoded_chunk_dir=f"{remote_output_dir}/decoded_chunks",
            remote_combined_csv=f"{remote_output_dir}/combined.csv",
            remote_manifest_json=f"{remote_receiver_run_dir}/manifest.json",
            remote_summary_json=f"{remote_receiver_run_dir}/summary.json",
            remote_helper_dir=remote_helper_dir,
            remote_validator=f"{remote_helper_dir}/validate_csv_against_reference.py",
        )

    def validate_config(self) -> None:
        if self.source_mode not in {"pcap_replay", "live_nic"}:
            raise PipelineError("source_mode must be pcap_replay or live_nic")
        if not self.slice_name and self.transport.transport_type == "loom":
            raise PipelineError("slice.name is required for Loom transport")
        if not self.receiver_node:
            raise PipelineError("slice.receiver_node is required")
        if not self.receiver_interface:
            raise PipelineError("network.receiver_interface is required")
        if not self.mapping_csv.startswith("/"):
            raise PipelineError("paths.mapping_csv must be an absolute receiver path")
        if self.source_mode == "pcap_replay":
            pcap = str(self.config.get("pcap_replay", {}).get("sender_pcap") or "")
            if not self.sender_node:
                raise PipelineError("slice.sender_node is required for pcap_replay")
            if not pcap.startswith("/"):
                raise PipelineError("pcap_replay.sender_pcap must be an absolute sender path")
            if not self.chunk_packets:
                raise PipelineError("decode.chunking.chunk_packets is required for pcap_replay")
        if self.source_mode == "live_nic":
            live = self.config.get("live_nic", {})
            if float(live.get("chunk_seconds") or 0) <= 0:
                raise PipelineError("live_nic.chunk_seconds must be positive")

    def remote_exec(self, node: str, command: str, timeout: int = 60, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = self.transport.exec(node, command, timeout)
        return self.check_remote_result(result, check=check, context=f"exec on {node}")

    def remote_copy_to(self, node: str, local: Path, remote: str, timeout: int = 120) -> None:
        result = self.transport.copy_to(node, local, remote, timeout)
        self.check_remote_result(result, check=True, context=f"copy_to {node}:{remote}")

    def remote_copy_from(self, node: str, remote: str, local: Path, timeout: int = 180) -> None:
        result = self.transport.copy_from(node, remote, local, timeout)
        self.check_remote_result(result, check=True, context=f"copy_from {node}:{remote}")

    def check_remote_result(
        self,
        result: subprocess.CompletedProcess[str],
        *,
        check: bool,
        context: str,
    ) -> subprocess.CompletedProcess[str]:
        if check and result.returncode != 0:
            args = result.args if isinstance(result.args, list) else [str(result.args)]
            raise PipelineError(
                f"remote {context} failed via {self.transport.transport_type}: "
                + shell_join(args)
                + f"\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        return result

    def prepare_local_dirs(self) -> None:
        self.layout.report_dir.mkdir(parents=True, exist_ok=True)
        self.layout.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.layout.viz_dir.mkdir(parents=True, exist_ok=True)
        self.layout.local_decoded_chunk_dir.mkdir(parents=True, exist_ok=True)

    def base_manifest(self, *, status: str, phase: str) -> dict[str, Any]:
        decode = self.config.get("decode", {})
        validation = self.config.get("validation", {})
        return {
            "manifest_type": "wib_fabric_live_demo_unified_manifest",
            "schema_version": 1,
            "architecture_mode": "queue_v2_adaptive_experimental"
            if decode.get("adaptive", {}).get("enabled")
            else "queue_v2_fixed",
            "status": status,
            "phase": phase,
            "updated_at": now_iso(),
            "run_id": self.layout.run_id,
            "source_mode": self.source_mode,
            "label": self.label,
            "slice_name": self.slice_name,
            "receiver_node": self.receiver_node,
            "sender_node": self.sender_node if self.source_mode == "pcap_replay" else None,
            "receiver_interface": self.receiver_interface,
            "transport": self.transport.describe(),
            "shared_pipeline": [
                "receiver NIC",
                "raw chunk queue",
                "decode/export",
                "manifest/status",
                "visualization",
                "optional validation",
            ],
            "input_adapter": self.source_mode,
            "chunking": {
                "mode": decode.get("chunking", {}).get("mode"),
                "chunk_packets": decode.get("chunking", {}).get("chunk_packets"),
                "live_chunk_seconds": self.config.get("live_nic", {}).get("chunk_seconds"),
                "adaptive": decode.get("adaptive", {}),
            },
            "paths": {
                "local_run_dir": str(self.layout.local_run_dir),
                "local_manifest": str(self.layout.manifest_json),
                "local_summary": str(self.layout.summary_json),
                "local_combined_csv": str(self.layout.local_combined_csv),
                "local_decoded_chunk_dir": str(self.layout.local_decoded_chunk_dir),
                "local_visualization_dir": str(self.layout.viz_dir),
                "remote_raw_chunk_dir": self.layout.remote_raw_chunk_dir,
                "remote_decoded_chunk_dir": self.layout.remote_decoded_chunk_dir,
                "remote_combined_csv": self.layout.remote_combined_csv,
                "remote_manifest": self.layout.remote_manifest_json,
                "remote_helper_dir": self.layout.remote_helper_dir,
                "remote_validator": self.layout.remote_validator,
            },
            "validation": {
                "enabled": self.validation_enabled,
                "reference_csv": validation.get("reference_csv") or None,
                "compare_fields": validation.get("compare_fields") or None,
                "expected_packets": validation.get("expected_packets"),
                "expected_rows": validation.get("expected_rows"),
            },
            "queue_metrics": self.queue_metrics(),
            "raw_chunks": [],
            "chunks": [],
        }

    def queue_metrics(self) -> dict[str, Any]:
        raw_count = len([chunk for chunk in self.chunks if chunk.get("raw_chunk_preserved")])
        decoded_count = len([chunk for chunk in self.chunks if chunk.get("decode_status") == "decoded"])
        appended_count = len([chunk for chunk in self.chunks if chunk.get("append_status") == "appended"])
        drops = sum(int(chunk.get("tcpdump_dropped_by_kernel") or 0) for chunk in self.chunks)
        return {
            "raw_chunk_preserved_count": raw_count,
            "decode_completed_chunk_count": decoded_count,
            "append_completed_chunk_count": appended_count,
            "queue_depth_chunks": max(0, raw_count - decoded_count),
            "raw_packet_drops": drops,
            "decode_workers_configured": int(self.config.get("decode", {}).get("queue", {}).get("decode_workers") or 1),
        }

    def write_manifest(self, *, status: str, phase: str, error: str | None = None) -> None:
        self.manifest.update(
            {
                "status": status,
                "phase": phase,
                "updated_at": now_iso(),
                "queue_metrics": self.queue_metrics(),
                "raw_chunks": [self.public_chunk(chunk) for chunk in self.chunks if chunk.get("raw_chunk_preserved")],
                "chunks": [self.public_chunk(chunk) for chunk in self.chunks],
            }
        )
        if error:
            self.manifest["error"] = error
        atomic_write_json(self.layout.manifest_json, self.manifest)

    def public_chunk(self, chunk: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in chunk.items() if not key.startswith("_")}

    def dry_run_payload(self, *, emit_json: bool = True) -> dict[str, Any]:
        self.prepare_local_dirs()
        self.write_manifest(status="dry_run", phase="planned")
        payload = {
            "dry_run": True,
            "config_path": str(self.config_path),
            "configuration": self.config,
            "transport": self.transport.describe(),
            "architecture": {
                "input_adapters": {
                    "pcap_replay": "tcpreplay on sender VM injects a config-selected PCAP into the receiver NIC",
                    "live_nic": "receiver VM captures rolling chunks from traffic already arriving on the NIC; no tcpreplay",
                },
                "shared_after_receiver_nic": self.manifest["shared_pipeline"],
            },
            "local_paths": {
                "run_dir": str(self.layout.local_run_dir),
                "manifest_json": str(self.layout.manifest_json),
                "summary_json": str(self.layout.summary_json),
                "dry_run_json": str(self.layout.dry_run_json),
                "combined_csv": str(self.layout.local_combined_csv),
                "decoded_chunk_dir": str(self.layout.local_decoded_chunk_dir),
                "visualization_dir": str(self.layout.viz_dir),
            },
            "remote_paths": self.manifest["paths"],
            "planned_commands": self.planned_commands(),
            "notes": [
                "Dry-run does not contact FABRIC VMs and does not run tcpreplay.",
                "Runtime outputs default to /tmp/wib_fabric_live_demo.",
                f"Selected transport backend: {self.transport.transport_type}.",
                "Adaptive mode is experimental and disabled unless requested in config or CLI.",
            ],
        }
        atomic_write_json(self.layout.dry_run_json, payload)
        if emit_json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        return payload

    def planned_commands(self) -> dict[str, Any]:
        common_decode = self.decode_command(0, "<RAW_CHUNK_PCAP>", "<CHUNK_CSV>")
        commands: dict[str, Any] = {
            "receiver_preflight": self.receiver_preflight_command(),
            "prepare_receiver_dirs": self.prepare_receiver_command(),
            "shared_decode_export": common_decode,
            "shared_append": self.append_command(0, "<CHUNK_CSV>"),
            "shared_validation": self.validation_command() if self.validation_enabled else None,
        }
        if self.source_mode == "pcap_replay":
            commands["input_adapter"] = {
                "split_sender_pcap": self.split_source_pcap_command(),
                "receiver_capture": self.pcap_receiver_capture_command(0, "<PACKETS>", "<RAW_CHUNK_PCAP>", "<TCPDUMP_LOG>"),
                "sender_replay": self.sender_replay_command({"index": 0, "packets": "<PACKETS>", "path": "<SOURCE_CHUNK_PCAP>"}),
            }
        else:
            commands["input_adapter"] = {
                "rolling_receiver_capture": self.live_capture_command(0, "<RAW_CHUNK_PCAP>", "<TCPDUMP_LOG>"),
            }
        return commands

    def receiver_preflight_command(self) -> str:
        return (
            "set -euo pipefail; "
            "command -v tcpdump >/dev/null; "
            "command -v python3 >/dev/null; "
            f"test -d {shlex.quote(self.wibviz_src)}; "
            f"test -f {shlex.quote(self.mapping_csv)}; "
            f"ip -brief addr show dev {shlex.quote(self.receiver_interface)}"
        )

    def sender_preflight_command(self) -> str:
        pcap = str(self.config.get("pcap_replay", {}).get("sender_pcap") or "")
        return (
            "set -euo pipefail; "
            "command -v tcpreplay >/dev/null; "
            f"test -f {shlex.quote(pcap)}; "
            f"ip -brief addr show dev {shlex.quote(self.sender_interface)}"
        )

    def prepare_receiver_command(self) -> str:
        return (
            "set -euo pipefail; "
            f"mkdir -p {shlex.quote(self.layout.remote_receiver_run_dir)} "
            f"{shlex.quote(self.layout.remote_raw_chunk_dir)} "
            f"{shlex.quote(self.layout.remote_decoded_chunk_dir)} "
            f"{shlex.quote(self.layout.remote_helper_dir)}"
        )

    def prepare_sender_command(self) -> str:
        return (
            "set -euo pipefail; "
            f"mkdir -p {shlex.quote(self.layout.remote_sender_run_dir)} "
            f"{shlex.quote(self.layout.remote_sender_run_dir + '/source_chunks')}"
        )

    def split_source_pcap_command(self) -> str:
        replay = self.config.get("pcap_replay", {})
        pcap = str(replay.get("sender_pcap") or "")
        packet_limit = replay.get("packet_limit")
        limit = 0 if packet_limit in (None, "") else int(packet_limit)
        chunk_packets = int(self.chunk_packets or 64)
        out_dir = f"{self.layout.remote_sender_run_dir}/source_chunks"
        script = r"""
import json
import struct
import sys
from pathlib import Path

src = Path(sys.argv[1])
out_dir = Path(sys.argv[2])
chunk_packets = int(sys.argv[3])
limit = int(sys.argv[4])
out_dir.mkdir(parents=True, exist_ok=True)
with src.open('rb') as handle:
    global_header = handle.read(24)
    if len(global_header) != 24:
        raise SystemExit('pcap global header is too short')
    magic = global_header[:4]
    if magic in (b'\xd4\xc3\xb2\xa1', b'\x4d\x3c\xb2\xa1'):
        endian = '<'
    elif magic in (b'\xa1\xb2\xc3\xd4', b'\xa1\xb2\x3c\x4d'):
        endian = '>'
    else:
        raise SystemExit(f'unsupported pcap magic: {magic!r}')
    manifest = []
    current = None
    current_path = None
    current_count = 0
    total = 0
    chunk_index = -1
    def close_current():
        global current, current_path, current_count
        if current is None:
            return
        current.close()
        manifest.append({'index': chunk_index, 'path': str(current_path), 'packets': current_count, 'bytes': current_path.stat().st_size})
        current = None
        current_path = None
        current_count = 0
    while limit <= 0 or total < limit:
        record_header = handle.read(16)
        if not record_header:
            break
        if len(record_header) != 16:
            raise SystemExit('truncated pcap record header')
        _ts_sec, _ts_usec, incl_len, _orig_len = struct.unpack(endian + 'IIII', record_header)
        payload = handle.read(incl_len)
        if len(payload) != incl_len:
            raise SystemExit('truncated pcap record payload')
        if current is None:
            chunk_index += 1
            current_path = out_dir / f'source_chunk_{chunk_index:03d}.pcap'
            current = current_path.open('wb')
            current.write(global_header)
        current.write(record_header)
        current.write(payload)
        current_count += 1
        total += 1
        if current_count >= chunk_packets:
            close_current()
    close_current()
print('CHUNK_MANIFEST:' + json.dumps({'chunks': manifest, 'total_packets': total}, separators=(',', ':')))
"""
        return (
            "set -euo pipefail; "
            + self.prepare_sender_command()
            + "; python3 -c "
            + shlex.quote(script)
            + " "
            + " ".join(shlex.quote(str(part)) for part in [pcap, out_dir, chunk_packets, limit])
        )

    def pcap_receiver_capture_command(self, index: int, packets: Any, remote_pcap: str, remote_log: str) -> str:
        completed_marker = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_capture_completed_at.txt"
        return (
            "set -euo pipefail; "
            "set +e; "
            f"sudo tcpdump -i {shlex.quote(self.receiver_interface)} -n -s 0 -B 8192 -U "
            f"-c {shlex.quote(str(packets))} -w {shlex.quote(remote_pcap)} "
            f"{shlex.quote(self.capture_filter)} > {shlex.quote(remote_log)} 2>&1; "
            "rc=$?; set -e; "
            f"date -u +%Y-%m-%dT%H:%M:%SZ > {shlex.quote(completed_marker)}; "
            "exit \"$rc\""
        )

    def sender_replay_command(self, chunk: dict[str, Any]) -> str:
        replay = self.config.get("pcap_replay", {})
        index = int(chunk["index"])
        packets = chunk["packets"]
        source_pcap = str(chunk["path"])
        remote_log = f"{self.layout.remote_sender_run_dir}/chunk_{index:03d}_tcpreplay.log"
        timing = str(replay.get("replay_timing") or "pps")
        rate_arg = "" if timing == "original" else f"--pps={int(replay.get('pps') or 50)} "
        extra = " ".join(shlex.quote(str(arg)) for arg in replay.get("tcpreplay_extra_args") or [])
        return (
            "set -euo pipefail; "
            f"tcpreplay -i {shlex.quote(self.sender_interface)} {rate_arg}"
            f"--limit={shlex.quote(str(packets))} {extra} {shlex.quote(source_pcap)} "
            f"> {shlex.quote(remote_log)} 2>&1"
        )

    def live_capture_command(self, index: int, remote_pcap: str, remote_log: str) -> str:
        seconds = float(self.config.get("live_nic", {}).get("chunk_seconds") or 5)
        completed_marker = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_capture_completed_at.txt"
        return (
            "set -euo pipefail; "
            "set +e; "
            f"sudo timeout --signal=INT --kill-after=5s {seconds:g}s "
            f"tcpdump -i {shlex.quote(self.receiver_interface)} -n -s 0 -B 8192 -U "
            f"-w {shlex.quote(remote_pcap)} {shlex.quote(self.capture_filter)} "
            f"> {shlex.quote(remote_log)} 2>&1; "
            "rc=$?; set -e; "
            "if [ \"$rc\" -ne 0 ] && [ \"$rc\" -ne 124 ]; then exit \"$rc\"; fi; "
            f"date -u +%Y-%m-%dT%H:%M:%SZ > {shlex.quote(completed_marker)}"
        )

    def decode_command(self, index: int, remote_pcap: str, remote_csv: str) -> str:
        decode = self.config.get("decode", {})
        return (
            "set -euo pipefail; "
            f"PYTHONPATH={shlex.quote(self.wibviz_src)} python3 -m wibviz.export "
            f"--pcap {shlex.quote(remote_pcap)} "
            f"--output {shlex.quote(remote_csv)} "
            f"--mapping {shlex.quote(self.mapping_csv)} "
            f"--tpc {shlex.quote(str(decode.get('tpc', 0)))} "
            f"--plane {shlex.quote(str(decode.get('plane', 0)))}"
        )

    def append_command(self, index: int, remote_csv: str) -> str:
        if index == 0:
            append = f"cp {shlex.quote(remote_csv)} {shlex.quote(self.layout.remote_combined_csv)}"
        else:
            append = f"tail -n +2 {shlex.quote(remote_csv)} >> {shlex.quote(self.layout.remote_combined_csv)}"
        return (
            "set -euo pipefail; "
            f"{append}; "
            f"wc -l < {shlex.quote(self.layout.remote_combined_csv)}"
        )

    def remote_timestamp_or_none(self, node: str, path: str) -> str | None:
        result = self.remote_exec(node, f"cat {shlex.quote(path)} 2>/dev/null || true", 30, False)
        value = result.stdout.strip().splitlines()[-1].strip() if result.stdout.strip() else ""
        return value or None

    def validation_command(self, reference_csv: str | None = None, report_path: str | None = None) -> str:
        validation = self.config.get("validation", {})
        compare = validation.get("compare_fields") or ""
        compare_arg = f" --compare-fields {shlex.quote(compare)}" if compare else ""
        reference = reference_csv or str(validation.get("reference_csv") or "")
        report = report_path or self.layout.remote_receiver_run_dir + "/validation.json"
        return (
            "set -euo pipefail; "
            f"python3 {shlex.quote(self.layout.remote_validator)} "
            f"--output {shlex.quote(self.layout.remote_combined_csv)} "
            f"--reference {shlex.quote(reference)} "
            f"--report {shlex.quote(report)}"
            f"{compare_arg}"
        )

    def run(self, *, dry_run: bool = False, emit_json: bool = True) -> dict[str, Any]:
        self.validate_config()
        if dry_run:
            return self.dry_run_payload(emit_json=emit_json)
        self.prepare_local_dirs()
        self.write_manifest(status="running", phase="preflight")
        error: Exception | None = None
        summary: dict[str, Any] = {}
        try:
            self.preflight()
            self.prepare_remote()
            self.write_manifest(status="running", phase="input_capture")
            if self.source_mode == "pcap_replay":
                self.run_pcap_replay()
            else:
                self.run_live_nic()
            self.write_manifest(status="running", phase="validation")
            validation = self.run_validation()
            self.write_manifest(status="running", phase="download")
            self.download_outputs()
            self.write_manifest(status="running", phase="visualization")
            viz = self.render_visualizations()
            run_pass = not validation.get("enabled", False) or bool(validation.get("pass"))
            summary = {
                "pass": run_pass,
                "run_id": self.layout.run_id,
                "source_mode": self.source_mode,
                "manifest": str(self.layout.manifest_json),
                "combined_csv": str(self.layout.local_combined_csv),
                "visualization": viz,
                "validation": validation,
                "queue_metrics": self.queue_metrics(),
            }
            self.manifest["validation_result"] = validation
            self.manifest["visualization"] = viz
            self.manifest["pass"] = run_pass
            self.write_manifest(status="complete", phase="complete")
        except Exception as exc:
            error = exc
            summary = {
                "pass": False,
                "run_id": self.layout.run_id,
                "source_mode": self.source_mode,
                "error": str(exc),
                "manifest": str(self.layout.manifest_json),
            }
            self.write_manifest(status="failed", phase="failed", error=str(exc))
        finally:
            summary.setdefault("config_path", str(self.config_path))
            summary.setdefault("local_run_dir", str(self.layout.local_run_dir))
            atomic_write_json(self.layout.summary_json, summary)
            if emit_json:
                print(json.dumps(summary, indent=2, sort_keys=True))
        if error:
            raise error
        return summary

    def preflight(self) -> None:
        receiver = self.remote_exec(self.receiver_node, self.receiver_preflight_command(), 30, True)
        expected_receiver_ip = str(self.config.get("network", {}).get("expected_receiver_ip") or "")
        if expected_receiver_ip and expected_receiver_ip not in receiver.stdout:
            raise PipelineError(f"receiver interface missing expected IP {expected_receiver_ip}: {receiver.stdout}")
        if self.source_mode == "pcap_replay":
            sender = self.remote_exec(self.sender_node, self.sender_preflight_command(), 30, True)
            expected_sender_ip = str(self.config.get("network", {}).get("expected_sender_ip") or "")
            if expected_sender_ip and expected_sender_ip not in sender.stdout:
                raise PipelineError(f"sender interface missing expected IP {expected_sender_ip}: {sender.stdout}")

    def prepare_remote(self) -> None:
        self.remote_exec(self.receiver_node, self.prepare_receiver_command(), 30, True)
        self.ensure_receiver_helper(VALIDATOR, self.layout.remote_validator)
        if self.source_mode == "pcap_replay":
            self.remote_exec(self.sender_node, self.prepare_sender_command(), 30, True)

    def remote_helper_matches(self, remote_path: str, local_path: Path) -> bool:
        expected = sha256(local_path)
        command = (
            f"test -s {shlex.quote(remote_path)} "
            f"&& sha256sum {shlex.quote(remote_path)} | awk '{{print $1}}' "
            "|| true"
        )
        result = self.remote_exec(self.receiver_node, command, 30, False)
        actual = result.stdout.strip().splitlines()[-1].strip() if result.stdout.strip() else ""
        return actual == expected

    def upload_helper_via_base64(self, local_path: Path, remote_path: str) -> None:
        encoded = base64.b64encode(local_path.read_bytes()).decode("ascii")
        tmp_path = f"/tmp/{self.layout.run_id}_{Path(remote_path).name}.b64"
        command = (
            "set -euo pipefail; "
            f"cat > {shlex.quote(tmp_path)} <<'EOF'\n"
            f"{encoded}\n"
            "EOF\n"
            f"base64 -d {shlex.quote(tmp_path)} > {shlex.quote(remote_path)}; "
            f"chmod 0755 {shlex.quote(remote_path)}; "
            f"rm -f {shlex.quote(tmp_path)}"
        )
        self.remote_exec(self.receiver_node, command, 120, True)

    def ensure_receiver_helper(self, local_path: Path, remote_path: str) -> None:
        if self.remote_helper_matches(remote_path, local_path):
            self.manifest["helper_deployment"] = {
                "reusable": True,
                "method": "existing",
                "remote_path": remote_path,
            }
            return
        self.remote_exec(self.receiver_node, f"mkdir -p {shlex.quote(self.layout.remote_helper_dir)}", 30, True)
        try:
            self.remote_copy_to(self.receiver_node, local_path, remote_path, 120)
            method = f"{self.transport.transport_type}_copy_to"
        except PipelineError as error:
            self.manifest["helper_scp_error"] = str(error)
            self.upload_helper_via_base64(local_path, remote_path)
            method = f"{self.transport.transport_type}_exec_base64"
        if not self.remote_helper_matches(remote_path, local_path):
            raise PipelineError(f"receiver helper deployment did not verify: {remote_path}")
        self.manifest["helper_deployment"] = {
            "reusable": True,
            "method": method,
            "remote_path": remote_path,
        }

    def run_pcap_replay(self) -> None:
        split = self.remote_exec(self.sender_node, self.split_source_pcap_command(), 120, True)
        source_manifest = parse_sentinel_json(split.stdout, "CHUNK_MANIFEST:")
        chunks = source_manifest.get("chunks") or []
        if not chunks:
            raise PipelineError("sender PCAP splitter produced no chunks")
        cumulative_rows = 0
        for chunk in chunks:
            index = int(chunk["index"])
            raw_pcap = f"{self.layout.remote_raw_chunk_dir}/receiver_chunk_{index:03d}.pcap"
            tcpdump_log = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_tcpdump.log"
            capture_cmd = self.pcap_receiver_capture_command(index, chunk["packets"], raw_pcap, tcpdump_log)
            remote_pid = self.layout.remote_receiver_run_dir + f"/chunk_{index:03d}_tcpdump.pid"
            start_log = self.layout.remote_receiver_run_dir + f"/chunk_{index:03d}_tcpdump_start.log"
            start_cmd = (
                "set -euo pipefail; "
                f"nohup bash -lc {shlex.quote(capture_cmd)} </dev/null > {shlex.quote(start_log)} 2>&1 & "
                f"echo $! > {shlex.quote(remote_pid)}; "
                "sleep 1; "
                f"kill -0 $(cat {shlex.quote(remote_pid)})"
            )
            self.remote_exec(self.receiver_node, start_cmd, 30, True)
            self.remote_exec(self.sender_node, self.sender_replay_command(chunk), self.timeout, True)
            wait_cmd = (
                f"pid=$(cat {shlex.quote(self.layout.remote_receiver_run_dir + f'/chunk_{index:03d}_tcpdump.pid')}); "
                "for i in $(seq 1 30); do kill -0 \"$pid\" 2>/dev/null || exit 0; sleep 1; done; "
                "kill -INT \"$pid\" 2>/dev/null || true"
            )
            self.remote_exec(self.receiver_node, wait_cmd, 45, False)
            self.capture_count_decode_append(index, raw_pcap, tcpdump_log, int(chunk["packets"]), cumulative_rows)
            cumulative_rows = int(self.chunks[-1].get("cumulative_rows") or cumulative_rows)

    def run_live_nic(self) -> None:
        live = self.config.get("live_nic", {})
        max_chunks = live.get("max_chunks")
        if max_chunks in (None, "") and live.get("require_follow_for_unbounded", True) and not self.follow:
            raise PipelineError("live_nic has no max_chunks; pass --follow for an unbounded run or set --max-live-chunks")
        stop_file = str(live.get("stop_file") or "/tmp/wib_fabric_live_demo.stop")
        cumulative_rows = 0
        index = 0
        while True:
            if max_chunks not in (None, "") and index >= int(max_chunks):
                break
            stop_check = self.remote_exec(self.receiver_node, f"test -f {shlex.quote(stop_file)}", 15, False)
            if stop_check.returncode == 0:
                break
            raw_pcap = f"{self.layout.remote_raw_chunk_dir}/receiver_chunk_{index:03d}.pcap"
            tcpdump_log = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_tcpdump.log"
            self.remote_exec(self.receiver_node, self.live_capture_command(index, raw_pcap, tcpdump_log), self.timeout, True)
            self.capture_count_decode_append(index, raw_pcap, tcpdump_log, None, cumulative_rows)
            cumulative_rows = int(self.chunks[-1].get("cumulative_rows") or cumulative_rows)
            index += 1

    def capture_count_decode_append(
        self,
        index: int,
        raw_pcap: str,
        tcpdump_log: str,
        packets_expected: int | None,
        cumulative_rows_before: int,
    ) -> None:
        count_cmd = f"tcpdump -nn -r {shlex.quote(raw_pcap)} {shlex.quote(self.capture_filter)} 2>/dev/null | wc -l"
        count_result = self.remote_exec(self.receiver_node, count_cmd, 60, True)
        packets_captured = parse_last_int(count_result.stdout)
        tcpdump_text = self.remote_exec(self.receiver_node, f"cat {shlex.quote(tcpdump_log)} 2>/dev/null || true", 30, False)
        capture_completed_marker = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_capture_completed_at.txt"
        receiver_capture_completed_at = self.remote_timestamp_or_none(self.receiver_node, capture_completed_marker)
        chunk_entry: dict[str, Any] = {
            "chunk_index": index,
            "raw_chunk_preserved": True,
            "raw_chunk_pcap_path": raw_pcap,
            "packets_expected": packets_expected,
            "packets_captured": packets_captured,
            "tcpdump_dropped_by_kernel": parse_tcpdump_stats(tcpdump_text.stdout).get("packets_dropped_by_kernel", 0),
            "receiver_capture_completed_at": receiver_capture_completed_at,
            "capture_completed_time": receiver_capture_completed_at,
            "capture_completed_marker_path": capture_completed_marker,
            "status": "queued_for_decode",
        }
        self.chunks.append(chunk_entry)
        self.write_manifest(status="running", phase="decode_export")

        remote_csv = f"{self.layout.remote_decoded_chunk_dir}/chunk_{index:03d}.csv"
        decode_log = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_decode.log"
        decode_started_marker = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_decode_started_at.txt"
        decode_completed_marker = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_decode_completed_at.txt"
        decode_cmd = (
            f"date -u +%Y-%m-%dT%H:%M:%SZ > {shlex.quote(decode_started_marker)}; "
            + self.decode_command(index, raw_pcap, remote_csv)
            + f" > {shlex.quote(decode_log)} 2>&1; "
            + f"date -u +%Y-%m-%dT%H:%M:%SZ > {shlex.quote(decode_completed_marker)}"
        )
        self.remote_exec(self.receiver_node, decode_cmd, self.timeout, True)
        decode_started_at = self.remote_timestamp_or_none(self.receiver_node, decode_started_marker)
        decode_completed_at = self.remote_timestamp_or_none(self.receiver_node, decode_completed_marker)
        decode_text = self.remote_exec(self.receiver_node, f"cat {shlex.quote(decode_log)}", 30, False)
        stats = parse_export_stats(decode_text.stdout)
        append_result = self.remote_exec(self.receiver_node, self.append_command(index, remote_csv), 120, True)
        combined_lines = parse_last_int(append_result.stdout)
        cumulative_rows = max(0, combined_lines - 1)
        local_chunk_csv = self.layout.local_decoded_chunk_dir / f"chunk_{index:03d}.csv"
        self.remote_copy_from(self.receiver_node, remote_csv, local_chunk_csv, 300)
        local_chunk_csv_available_at = now_iso()
        chunk_entry.update(
            {
                "status": "appended",
                "decode_status": "decoded",
                "append_status": "appended",
                "decoded_csv_path": remote_csv,
                "local_decoded_csv_path": str(local_chunk_csv),
                "local_decoded_csv_size": local_chunk_csv.stat().st_size if local_chunk_csv.exists() else None,
                "decode_started_at": decode_started_at,
                "decode_started_marker_path": decode_started_marker,
                "decode_completed_at": decode_completed_at,
                "decode_completed_time": decode_completed_at,
                "decode_completed_marker_path": decode_completed_marker,
                "local_chunk_csv_available_at": local_chunk_csv_available_at,
                "local_copy_completed_time": local_chunk_csv_available_at,
                "decode_export_statistics": stats,
                "combined_lines_after_append": combined_lines,
                "cumulative_rows_before": cumulative_rows_before,
                "cumulative_rows": cumulative_rows,
                "rows_appended": cumulative_rows - cumulative_rows_before,
            }
        )
        self.write_manifest(status="running", phase="manifest_status")

    def run_validation(self) -> dict[str, Any]:
        if not self.validation_enabled:
            return {"enabled": False, "pass": None, "reason": "validation disabled or no reference CSV configured"}

        validation = self.config.get("validation", {})
        reference_csv = str(validation.get("reference_csv") or "")
        report_path = f"{self.layout.remote_receiver_run_dir}/validation.json"
        partial_validation: dict[str, Any] | None = None

        expected_packets = validation.get("expected_packets")
        decoded_packets = sum(
            int((chunk.get("decode_export_statistics") or {}).get("packets_read") or 0)
            for chunk in self.chunks
        )
        decoded_rows = int(self.chunks[-1].get("cumulative_rows") or 0) if self.chunks else 0
        if (
            self.source_mode == "pcap_replay"
            and expected_packets not in (None, "")
            and decoded_packets > 0
            and decoded_packets < int(expected_packets)
        ):
            reference_csv = f"{self.layout.remote_receiver_run_dir}/partial_reference_{decoded_packets:06d}pkt.csv"
            expected_lines = decoded_rows + 1
            self.remote_exec(
                self.receiver_node,
                "set -euo pipefail; "
                f"head -n {expected_lines} {shlex.quote(str(validation.get('reference_csv')))} > {shlex.quote(reference_csv)}; "
                f"test $(wc -l < {shlex.quote(reference_csv)}) -eq {expected_lines}",
                60,
                True,
            )
            partial_validation = {
                "enabled": True,
                "reason": "bounded replay smoke test compared against a reference CSV prefix",
                "decoded_packets": decoded_packets,
                "full_event_expected_packets": int(expected_packets),
                "decoded_rows": decoded_rows,
                "partial_reference_csv": reference_csv,
            }

        result = self.remote_exec(self.receiver_node, self.validation_command(reference_csv=reference_csv, report_path=report_path), self.timeout, False)
        text = self.remote_exec(self.receiver_node, f"cat {shlex.quote(report_path)} 2>/dev/null || true", 30, False)
        try:
            report = extract_json_object(text.stdout)
        except (ValueError, json.JSONDecodeError) as error:
            report = {
                "enabled": True,
                "pass": False,
                "error": f"validation report was not valid JSON: {error}",
                "returncode": result.returncode,
                "stdout_tail": text.stdout[-1000:],
            }
        if partial_validation:
            report["partial_validation"] = partial_validation
        report["enabled"] = True
        report.setdefault("returncode", result.returncode)
        return report

    def download_outputs(self) -> None:
        self.remote_copy_from(self.receiver_node, self.layout.remote_combined_csv, self.layout.local_combined_csv, 300)

    def render_visualizations(self) -> dict[str, Any]:
        if not self.config.get("visualization", {}).get("enabled", True):
            return {"enabled": False}
        if not self.layout.local_combined_csv.is_file():
            return {"enabled": True, "created": [], "error": "combined CSV is not available locally"}
        try:
            import matplotlib

            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            import pandas as pd
        except Exception as error:
            return {"enabled": True, "created": [], "error": f"matplotlib/pandas unavailable: {error}"}

        max_rows = int(self.config.get("visualization", {}).get("max_rows") or 500000)
        frame = pd.DataFrame(load_csv_sample(self.layout.local_combined_csv, max_rows))
        created: list[str] = []
        if frame.empty:
            return {"enabled": True, "created": [], "error": "combined CSV has no rows"}
        for column in ("wire_id", "tick", "adc_value"):
            if column in frame:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        outputs = self.config.get("visualization", {}).get("outputs") or []
        if "heatmap" in outputs and {"wire_id", "tick", "adc_value"}.issubset(frame.columns):
            pivot = frame.pivot_table(index="wire_id", columns="tick", values="adc_value", aggfunc="mean")
            heatmap_path = self.layout.viz_dir / "heatmap.png"
            plt.figure(figsize=(12, 7))
            plt.imshow(pivot.fillna(0).to_numpy(), aspect="auto", interpolation="nearest", cmap="magma")
            plt.title(f"{self.label} heatmap")
            plt.xlabel("tick")
            plt.ylabel("wire")
            plt.colorbar(label="ADC")
            plt.tight_layout()
            plt.savefig(heatmap_path, dpi=140)
            plt.close()
            created.append(str(heatmap_path))
        if "stacked_waveform" in outputs and {"wire_id", "tick", "adc_value"}.issubset(frame.columns):
            waveform_path = self.layout.viz_dir / "stacked_waveform.png"
            wires = sorted(int(value) for value in frame["wire_id"].dropna().unique())[:12]
            plt.figure(figsize=(12, 7))
            for offset, wire in enumerate(wires):
                subset = frame[frame["wire_id"] == wire].sort_values("tick").head(256)
                if subset.empty:
                    continue
                values = pd.to_numeric(subset["adc_value"], errors="coerce").fillna(0)
                plt.plot(subset["tick"], values + offset * max(1, values.std() * 4), linewidth=0.8, label=str(wire))
            plt.title(f"{self.label} stacked waveform")
            plt.xlabel("tick")
            plt.ylabel("ADC + offset")
            plt.tight_layout()
            plt.savefig(waveform_path, dpi=140)
            plt.close()
            created.append(str(waveform_path))
        return {"enabled": True, "created": created}


def output_root_for_config(config: dict[str, Any], output_root: Path | None) -> Path:
    root_value = output_root or Path(str(config.get("paths", {}).get("output_root") or "/tmp/wib_fabric_live_demo"))
    return root_value.expanduser().resolve()


def queue_tracking_from_manifest(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    raw = 0
    decoded = 0
    tracking: list[dict[str, Any]] = []
    for chunk in manifest.get("chunks", []):
        if chunk.get("raw_chunk_preserved"):
            raw += 1
        if chunk.get("decode_status") == "decoded":
            decoded += 1
        lag = max(0, raw - decoded)
        tracking.append(
            {
                "chunk_index": chunk.get("chunk_index"),
                "raw_chunks_seen": raw,
                "decoded_chunks_seen": decoded,
                "queue_depth_chunks": lag,
                "decode_lag_chunks": lag,
                "rows_appended": chunk.get("rows_appended"),
                "cumulative_rows": chunk.get("cumulative_rows"),
            }
        )
    return tracking


def int_metric(metrics: dict[str, Any], key: str) -> int:
    value = metrics.get(key)
    return 0 if value in (None, "") else int(value)


def repeated_totals(repeats: list[dict[str, Any]]) -> dict[str, Any]:
    totals = {
        "repeat_count": len(repeats),
        "completed_repeat_count": len([entry for entry in repeats if entry.get("status") in {"dry_run", "complete"}]),
        "failed_repeat_count": len([entry for entry in repeats if entry.get("status") == "failed"]),
        "raw_chunk_preserved_count": 0,
        "decode_completed_chunk_count": 0,
        "append_completed_chunk_count": 0,
        "raw_packet_drops": 0,
        "max_queue_depth_chunks": 0,
        "max_decode_lag_chunks": 0,
        "total_rows": 0,
    }
    for entry in repeats:
        metrics = entry.get("queue_metrics") or {}
        totals["raw_chunk_preserved_count"] += int_metric(metrics, "raw_chunk_preserved_count")
        totals["decode_completed_chunk_count"] += int_metric(metrics, "decode_completed_chunk_count")
        totals["append_completed_chunk_count"] += int_metric(metrics, "append_completed_chunk_count")
        totals["raw_packet_drops"] += int_metric(metrics, "raw_packet_drops")
        totals["max_queue_depth_chunks"] = max(
            totals["max_queue_depth_chunks"],
            int_metric(metrics, "queue_depth_chunks"),
        )
        for point in entry.get("queue_tracking") or []:
            totals["max_queue_depth_chunks"] = max(totals["max_queue_depth_chunks"], int(point.get("queue_depth_chunks") or 0))
            totals["max_decode_lag_chunks"] = max(totals["max_decode_lag_chunks"], int(point.get("decode_lag_chunks") or 0))
        totals["total_rows"] += int(entry.get("decoded_rows") or 0)
    return totals


def write_repeated_summary(series_dir: Path, payload: dict[str, Any]) -> None:
    atomic_write_json(series_dir / "reports" / "repeated_replay_summary.json", payload)


def run_repeated_replay(args: argparse.Namespace, config_paths: list[Path]) -> dict[str, Any]:
    if args.repeat < 1:
        raise PipelineError("--repeat must be at least 1")
    if args.follow:
        raise PipelineError("--repeat is for bounded pcap_replay; do not combine it with --follow")
    if args.repeat_config and args.repeat < 2:
        raise PipelineError("--repeat-config requires --repeat 2 or higher")

    resolved_paths = [path.expanduser().resolve() for path in config_paths]
    base_config = load_config(resolved_paths[0])
    series_output_root = output_root_for_config(base_config, args.output_root)
    series_id = args.run_id or f"{stamp()}-repeated-replay"
    series_dir = series_output_root / "runs" / series_id
    (series_dir / "reports").mkdir(parents=True, exist_ok=True)

    repeats: list[dict[str, Any]] = []
    payload: dict[str, Any] = {
        "summary_type": "wib_fabric_live_demo_repeated_replay_summary",
        "schema_version": 1,
        "status": "dry_run" if args.dry_run else "running",
        "dry_run": bool(args.dry_run),
        "series_id": series_id,
        "series_dir": str(series_dir),
        "repeat_count": args.repeat,
        "config_sequence": [str(path) for path in resolved_paths],
        "alternating_configs": len(resolved_paths) > 1,
        "notes": [
            "Repeated replay approximates a longer stream by running bounded pcap_replay repeatedly.",
            "Each repeat has its own remote/local run directory and manifest.",
            "Queue depth and decode lag are aggregated from per-repeat manifests; this is not identical to one continuous detector stream.",
        ],
        "repeats": repeats,
        "total_metrics": repeated_totals(repeats),
    }
    write_repeated_summary(series_dir, payload)

    try:
        for index in range(args.repeat):
            config_path = resolved_paths[index % len(resolved_paths)]
            config = load_config(config_path)
            label = slug(str(config.get("label") or config.get("event_id") or config_path.stem))
            repeat_run_id = f"{series_id}-repeat{index + 1:03d}-{label}"
            runner = PipelineRunner(
                copy.deepcopy(config),
                config_path=config_path,
                source_mode=args.source_mode,
                run_id=repeat_run_id,
                output_root=series_output_root,
                adaptive=True if args.adaptive else None,
                validate=False if args.no_validate else None,
                max_live_chunks=args.max_live_chunks,
                follow=False,
                timeout=args.timeout,
            )
            runner.validate_config()
            if runner.source_mode != "pcap_replay":
                raise PipelineError("--repeat supports pcap_replay configs only")

            entry: dict[str, Any] = {
                "repeat_index": index + 1,
                "run_id": repeat_run_id,
                "config_path": str(config_path),
                "source_mode": runner.source_mode,
                "label": runner.label,
                "dry_run": bool(args.dry_run),
                "status": "planned" if args.dry_run else "running",
            }
            repeats.append(entry)
            write_repeated_summary(series_dir, payload)

            result = runner.run(dry_run=args.dry_run, emit_json=False)
            manifest_path = Path(result.get("manifest") or result.get("local_paths", {}).get("manifest_json") or runner.layout.manifest_json)
            manifest: dict[str, Any] = {}
            if manifest_path.is_file():
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

            tracking = queue_tracking_from_manifest(manifest)
            chunks = manifest.get("chunks") or []
            decoded_rows = int(chunks[-1].get("cumulative_rows") or 0) if chunks else 0
            entry.update(
                {
                    "status": "dry_run" if args.dry_run else "complete",
                    "pass": result.get("pass") if not args.dry_run else None,
                    "manifest": str(manifest_path),
                    "local_run_dir": str(runner.layout.local_run_dir),
                    "queue_metrics": manifest.get("queue_metrics") or result.get("queue_metrics") or {},
                    "validation": manifest.get("validation_result") or result.get("validation"),
                    "chunk_count": len(chunks),
                    "decoded_rows": decoded_rows,
                    "queue_tracking": tracking,
                }
            )
            payload["status"] = "dry_run" if args.dry_run else "running"
            payload["total_metrics"] = repeated_totals(repeats)
            write_repeated_summary(series_dir, payload)
    except Exception as error:
        if repeats:
            repeats[-1]["status"] = "failed"
            repeats[-1]["error"] = str(error)
        payload["status"] = "failed"
        payload["error"] = str(error)
        payload["total_metrics"] = repeated_totals(repeats)
        write_repeated_summary(series_dir, payload)
        raise

    payload["status"] = "dry_run" if args.dry_run else "complete"
    payload["total_metrics"] = repeated_totals(repeats)
    write_repeated_summary(series_dir, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified WIB/FABRIC live demo runner.")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--source-mode", choices=["pcap_replay", "live_nic"], default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--adaptive", action="store_true", help="Enable experimental adaptive mode for this run.")
    parser.add_argument("--no-validate", action="store_true", help="Disable validation even if the config enables it.")
    parser.add_argument(
        "--max-live-chunks",
        type=int,
        default=None,
        help="Bound live_nic chunks; for pcap_replay smoke tests, cap replay to this many fixed chunks.",
    )
    parser.add_argument("--follow", action="store_true", help="Allow live_nic to run without max_live_chunks until stop_file appears.")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--repeat", type=int, default=1, help="Run bounded pcap_replay repeatedly to approximate a longer stream.")
    parser.add_argument(
        "--repeat-config",
        action="append",
        type=Path,
        default=[],
        help="Additional config path to alternate with --config across repeated replay runs. Can be used more than once.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config_path = args.config.expanduser().resolve()
    config = load_config(config_path)
    try:
        repeat_config_paths = [path.expanduser().resolve() for path in args.repeat_config]
        if args.repeat != 1 or repeat_config_paths:
            run_repeated_replay(args, [config_path, *repeat_config_paths])
            return 0
        runner = PipelineRunner(
            config,
            config_path=config_path,
            source_mode=args.source_mode,
            run_id=args.run_id,
            output_root=args.output_root,
            adaptive=True if args.adaptive else None,
            validate=False if args.no_validate else None,
            max_live_chunks=args.max_live_chunks,
            follow=args.follow,
            timeout=args.timeout,
        )
        runner.run(dry_run=args.dry_run)
    except (PipelineError, TransportConfigError) as error:
        print(f"run_pipeline: error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

