#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import base64
import csv
import hashlib
import json
import os
import queue
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from remote_transport import TransportConfigError, build_transport, shell_join

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = Path(__file__).resolve().with_name("validate_csv_against_reference.py")
RECEIVER_DECODE_WORKER = Path(__file__).resolve().with_name("receiver_decode_export_worker.py")
REPORT_NAME = "unified_queue_pipeline"
CSV_FIELDS = ("seq_id", "timestamp", "tick", "adc_value", "wire_id", "tpc", "slot", "plane", "packet_file")
RUN_REPORT_FILES = (
    "summary.json",
    "manifest.json",
    "measurement_summary.csv",
    "queue_metrics_history.csv",
    "validation_result.json",
    "dry_run.json",
)
SERIES_REPORT_FILES = ("repeated_replay_summary.json", "repeated_replay_metrics.csv")


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
    validation_json: Path
    local_combined_csv: Path
    queue_history_csv: Path
    measurement_csv: Path
    report_mirror_dir: Path | None
    remote_receiver_run_dir: str
    remote_sender_run_dir: str
    remote_raw_chunk_dir: str
    remote_decoded_chunk_dir: str
    remote_combined_csv: str
    remote_manifest_json: str
    remote_summary_json: str
    remote_helper_dir: str
    remote_validator: str
    remote_decode_worker: str
    remote_decode_task_dir: str
    remote_decode_status_dir: str
    remote_decode_worker_dir: str


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


def parse_sender_stats(text: str) -> dict[str, Any]:
    stats: dict[str, Any] = {"summary_found": False}
    actual = re.search(r"Actual:\s+(\d+) packets \((\d+) bytes\) sent in ([0-9.]+) seconds", text)
    if actual:
        stats.update(
            {
                "summary_found": True,
                "actual_packets": int(actual.group(1)),
                "actual_bytes": int(actual.group(2)),
                "duration_seconds": float(actual.group(3)),
            }
        )
    for label, key in [
        ("Successful packets", "successful_packets"),
        ("Failed packets", "failed_packets"),
        ("Truncated packets", "truncated_packets"),
        ("Retried packets \(ENOBUFS\)", "retried_enobufs"),
        ("Retried packets \(EAGAIN\)", "retried_eagain"),
    ]:
        match = re.search(label + r":\s+(\d+)", text)
        if match:
            stats[key] = int(match.group(1))
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


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    fraction = rank - low
    return round((ordered[low] * (1.0 - fraction)) + (ordered[high] * fraction), 3)


def float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def int_or_zero(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def metric_stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "min": None,
            "mean": None,
            "p50": None,
            "p95": None,
            "max": None,
            "total": 0.0,
        }
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": round(ordered[0], 3),
        "mean": round(sum(ordered) / len(ordered), 3),
        "p50": percentile(ordered, 50),
        "p95": percentile(ordered, 95),
        "max": round(ordered[-1], 3),
        "total": round(sum(ordered), 3),
    }


def chunk_once_accounting(expected_chunk_ids: list[int], observed_chunk_ids: list[int]) -> dict[str, Any]:
    expected = [int(value) for value in expected_chunk_ids]
    observed = [int(value) for value in observed_chunk_ids]
    expected_counter = Counter(expected)
    observed_counter = Counter(observed)
    missing_counter = expected_counter - observed_counter
    unexpected_counter = observed_counter - expected_counter
    duplicate_ids = sorted(chunk_id for chunk_id, count in observed_counter.items() if count > 1)
    return {
        "expected_count": len(expected),
        "observed_count": len(observed),
        "expected_chunk_ids": expected,
        "observed_chunk_ids": observed,
        "missing_chunk_ids": sorted(missing_counter.elements()),
        "duplicate_chunk_ids": duplicate_ids,
        "unexpected_chunk_ids": sorted(unexpected_counter.elements()),
        "every_expected_chunk_exactly_once": expected_counter == observed_counter,
    }


def write_csv_rows(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: "" if value is None else value for key, value in row.items()})


def resolve_project_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = PACKAGE_ROOT.parent / path
    return path.resolve()


def report_mirror_root_for_config(config: dict[str, Any]) -> Path | None:
    reporting = config.get("reporting") or {}
    paths = config.get("paths") or {}
    value = reporting.get("report_mirror_root") or paths.get("report_mirror_root")
    if value in (None, ""):
        return None
    return resolve_project_path(str(value))


def config_bool(value: Any, *, default: bool) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off"}
    return bool(value)


def mirror_series_reports_for_config(config: dict[str, Any]) -> bool:
    reporting = config.get("reporting") or {}
    return config_bool(reporting.get("mirror_series_reports"), default=True)


def mirror_per_repeat_reports_for_config(config: dict[str, Any]) -> bool:
    reporting = config.get("reporting") or {}
    # Backward-compatible default preserves prior behavior unless a run opts out.
    return config_bool(reporting.get("mirror_per_repeat_reports"), default=True)


def next_available_dir(path: Path) -> Path:
    if not path.exists() or not any(path.iterdir()):
        return path
    for suffix in range(2, 1000):
        candidate = path.with_name(f"{path.name}-{suffix:03d}")
        if not candidate.exists() or not any(candidate.iterdir()):
            return candidate
    raise PipelineError(f"could not find an unused report mirror directory for {path}")


def mirror_report_files(source_dir: Path, mirror_dir: Path | None, filenames: tuple[str, ...]) -> list[str]:
    if mirror_dir is None:
        return []
    copied: list[str] = []
    mirror_dir.mkdir(parents=True, exist_ok=True)
    for filename in filenames:
        source = source_dir / filename
        if not source.is_file():
            continue
        target = mirror_dir / filename
        shutil.copy2(source, target)
        copied.append(str(target))
    return copied


def dry_metric_stats() -> dict[str, Any]:
    return {
        "count": 0,
        "min": 0.0,
        "mean": 0.0,
        "p50": 0.0,
        "p95": 0.0,
        "max": 0.0,
        "total": 0.0,
        "dry_run_placeholder": True,
    }


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
        pipeline_mode: str | None = None,
        persistent_batch_chunks: int | None = None,
        persistent_batch_collect_timeout_seconds: float | None = None,
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
                experiment = self.config.setdefault("experiment", {})
                original_packets = experiment.get("packets_per_repeat")
                if original_packets not in (None, "") and int(original_packets) != requested_limit:
                    experiment.setdefault("full_packets_per_repeat", int(original_packets))
                experiment["packets_per_repeat"] = requested_limit
                experiment["packets_per_repeat_source"] = "--max-live-chunks compatibility bound for replay smoke tests"
            else:
                self.config.setdefault("live_nic", {})["max_chunks"] = max_live_chunks
        if pipeline_mode:
            self.config.setdefault("decode", {}).setdefault("queue", {})["pipeline_mode"] = pipeline_mode
        queue_config = self.config.setdefault("decode", {}).setdefault("queue", {})
        if persistent_batch_chunks is not None:
            queue_config["persistent_batch_chunks"] = int(persistent_batch_chunks)
        if persistent_batch_collect_timeout_seconds is not None:
            queue_config["persistent_batch_collect_timeout_seconds"] = float(persistent_batch_collect_timeout_seconds)
        self.timeout = timeout
        self.follow = follow
        self.layout = self.build_layout(run_id=run_id, output_root=output_root)
        self.transport = build_transport(
            self.config.get("transport"),
            slice_name=self.slice_name,
            log_file=self.layout.command_log,
        )
        self.chunks: list[dict[str, Any]] = []
        self.queue_metrics_history: list[dict[str, Any]] = []
        self.max_queue_depth_chunks = 0
        self.max_ordered_write_buffer_depth_chunks = 0
        self.queue_capacity_chunks = self.configured_queue_capacity_chunks()
        self.backpressure_metrics = self.initial_backpressure_metrics()
        self._state_lock = threading.RLock()
        self._chunk_condition = threading.Condition(self._state_lock)
        self.decode_worker_assignments: list[dict[str, Any]] = []
        self.persistent_decode_workers: list[dict[str, Any]] = []
        self.persistent_decode_worker_failures: list[dict[str, Any]] = []
        self.persistent_decode_batches: list[dict[str, Any]] = []
        self.persistent_decode_result_records: list[dict[str, Any]] = []
        self.persistent_decode_fallback_used = False
        self.output_merge_records: list[dict[str, Any]] = []
        self.ordered_output_records: list[dict[str, Any]] = []
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
    def pipeline_mode(self) -> str:
        return str(self.config.get("decode", {}).get("queue", {}).get("pipeline_mode") or "staged").strip().lower()

    @property
    def decode_workers(self) -> int:
        value = self.config.get("decode", {}).get("queue", {}).get("decode_workers")
        return 1 if value in (None, "") else int(value)

    @property
    def decode_worker_mode(self) -> str:
        queue_config = self.config.get("decode", {}).get("queue", {})
        raw_mode = str(
            queue_config.get("decode_worker_mode")
            or queue_config.get("worker_mode")
            or ""
        ).strip().lower()
        if not raw_mode:
            if self.pipeline_mode == "concurrent" and self.decode_workers == 1:
                return "persistent_receiver_process"
            return "per_chunk_command"
        aliases = {
            "persistent": "persistent_receiver_process",
            "persistent_receiver": "persistent_receiver_process",
            "persistent_receiver_process": "persistent_receiver_process",
            "receiver_persistent": "persistent_receiver_process",
            "per_chunk": "per_chunk_command",
            "per_chunk_command": "per_chunk_command",
            "command": "per_chunk_command",
        }
        return aliases.get(raw_mode, raw_mode)

    @property
    def persistent_decode_enabled(self) -> bool:
        return self.decode_worker_mode == "persistent_receiver_process"

    @property
    def output_write_strategy(self) -> str:
        output_config = self.config.get("output") or {}
        queue_config = self.config.get("decode", {}).get("queue", {})
        raw_strategy = str(
            output_config.get("write_strategy")
            or queue_config.get("output_write_strategy")
            or "ordered_append"
        ).strip().lower()
        aliases = {
            "append": "ordered_append",
            "ordered_append": "ordered_append",
            "single_ordered_append": "ordered_append",
            "merge": "ordered_chunk_merge",
            "ordered_merge": "ordered_chunk_merge",
            "chunk_merge": "ordered_chunk_merge",
            "ordered_chunk_merge": "ordered_chunk_merge",
            "per_chunk_merge": "ordered_chunk_merge",
        }
        return aliases.get(raw_strategy, raw_strategy)

    @property
    def persistent_decode_batch_chunks(self) -> int:
        queue_config = self.config.get("decode", {}).get("queue", {})
        value = queue_config.get("persistent_batch_chunks") or queue_config.get("batch_chunks") or 1
        return max(1, int(value))

    @property
    def persistent_decode_batch_collect_timeout_seconds(self) -> float:
        queue_config = self.config.get("decode", {}).get("queue", {})
        value = queue_config.get("persistent_batch_collect_timeout_seconds")
        if value in (None, ""):
            value = queue_config.get("batch_collect_timeout_seconds", 0.0)
        return max(0.0, float(value))

    def configured_queue_capacity_chunks(self) -> int | None:
        queue_config = self.config.get("decode", {}).get("queue", {})
        for key in ("capacity_chunks", "queue_capacity_chunks", "capacity"):
            value = queue_config.get(key)
            if value not in (None, ""):
                return int(value)
        return None

    def queue_maxsize(self) -> int:
        return int(self.queue_capacity_chunks or 0)

    def new_pipeline_queue(self) -> queue.Queue[dict[str, Any] | None]:
        return queue.Queue(maxsize=self.queue_maxsize())

    def initial_backpressure_metrics(self) -> dict[str, dict[str, Any]]:
        capacity = self.queue_capacity_chunks

        def channel(name: str) -> dict[str, Any]:
            return {
                "name": name,
                "capacity_chunks": capacity,
                "bounded": bool(capacity),
                "put_count": 0,
                "get_count": 0,
                "blocked_put_count": 0,
                "total_blocked_put_seconds": 0.0,
                "total_put_elapsed_seconds": 0.0,
                "max_put_elapsed_seconds": 0.0,
                "max_depth_after_put": 0,
                "current_depth": 0,
                "events": [],
            }

        return {
            "raw_to_decode": channel("bounded raw chunks before decode"),
            "decoded_to_writer": channel("bounded decoded chunks before ordered writing"),
        }

    def put_with_backpressure(
        self,
        target_queue: queue.Queue[dict[str, Any] | None],
        item: dict[str, Any],
        channel: str,
        *,
        chunk_index: int | None,
    ) -> None:
        started = time.monotonic()
        was_full = target_queue.maxsize > 0 and target_queue.full()
        target_queue.put(item)
        elapsed = time.monotonic() - started
        depth = target_queue.qsize()
        blocked = bool(was_full or (target_queue.maxsize > 0 and elapsed >= 0.001))
        with self._state_lock:
            stats = self.backpressure_metrics[channel]
            stats["put_count"] += 1
            stats["total_put_elapsed_seconds"] = round(float(stats["total_put_elapsed_seconds"]) + elapsed, 6)
            stats["max_put_elapsed_seconds"] = round(max(float(stats["max_put_elapsed_seconds"]), elapsed), 6)
            stats["max_depth_after_put"] = max(int(stats["max_depth_after_put"]), depth)
            stats["current_depth"] = depth
            if blocked:
                stats["blocked_put_count"] += 1
                stats["total_blocked_put_seconds"] = round(float(stats["total_blocked_put_seconds"]) + elapsed, 6)
                events = stats.setdefault("events", [])
                events.append(
                    {
                        "time": now_iso(),
                        "chunk_index": chunk_index,
                        "blocked_seconds": round(elapsed, 6),
                        "depth_after_put": depth,
                    }
                )
                if len(events) > 100:
                    del events[:-100]

    def note_queue_get(self, channel: str, target_queue: queue.Queue[dict[str, Any] | None]) -> None:
        with self._state_lock:
            stats = self.backpressure_metrics[channel]
            stats["get_count"] += 1
            stats["current_depth"] = target_queue.qsize()

    @property
    def validation_enabled(self) -> bool:
        validation = self.config.get("validation", {})
        return bool(validation.get("enabled")) and bool(validation.get("reference_csv"))

    def build_layout(self, *, run_id: str | None, output_root: Path | None) -> Layout:
        root_value = output_root or Path(str(self.config.get("paths", {}).get("output_root") or "/tmp/iceberg_fabric_runs"))
        root = root_value.expanduser().resolve()
        final_run_id = run_id or f"{stamp()}-{self.label}-{self.source_mode}"
        local_run_dir = root / "runs" / final_run_id
        report_dir = local_run_dir / "reports"
        artifact_dir = local_run_dir / "artifacts"
        viz_dir = local_run_dir / "visualizations"
        local_decoded_chunk_dir = artifact_dir / "decoded_chunks"
        remote_receiver_run_dir = f"{self.receiver_root}/wibviz_runs/iceberg_fabric/{final_run_id}"
        remote_helper_dir = f"{self.receiver_root}/wibviz_runs/iceberg_fabric/helpers"
        remote_sender_run_dir = f"{self.sender_root}/wibviz_runs/iceberg_fabric/{final_run_id}"
        remote_output_dir = f"{self.receiver_root}/wibviz_outputs/iceberg_fabric/{final_run_id}"
        report_mirror_root = report_mirror_root_for_config(self.config)
        report_mirror_dir = (
            next_available_dir(report_mirror_root / final_run_id / "reports")
            if report_mirror_root is not None and mirror_per_repeat_reports_for_config(self.config)
            else None
        )
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
            validation_json=report_dir / "validation_result.json",
            local_combined_csv=artifact_dir / "combined.csv",
            queue_history_csv=report_dir / "queue_metrics_history.csv",
            measurement_csv=report_dir / "measurement_summary.csv",
            report_mirror_dir=report_mirror_dir,
            remote_receiver_run_dir=remote_receiver_run_dir,
            remote_sender_run_dir=remote_sender_run_dir,
            remote_raw_chunk_dir=f"{remote_output_dir}/raw_chunks",
            remote_decoded_chunk_dir=f"{remote_output_dir}/decoded_chunks",
            remote_combined_csv=f"{remote_output_dir}/combined.csv",
            remote_manifest_json=f"{remote_receiver_run_dir}/manifest.json",
            remote_summary_json=f"{remote_receiver_run_dir}/summary.json",
            remote_helper_dir=remote_helper_dir,
            remote_validator=f"{remote_helper_dir}/validate_csv_against_reference.py",
            remote_decode_worker=f"{remote_helper_dir}/receiver_decode_export_worker.py",
            remote_decode_task_dir=f"{remote_receiver_run_dir}/decode_tasks",
            remote_decode_status_dir=f"{remote_receiver_run_dir}/decode_status",
            remote_decode_worker_dir=f"{remote_receiver_run_dir}/decode_workers",
        )

    def validate_config(self) -> None:
        if self.source_mode not in {"pcap_replay", "live_nic"}:
            raise PipelineError("source_mode must be pcap_replay or live_nic")
        if self.pipeline_mode not in {"staged", "concurrent"}:
            raise PipelineError("decode.queue.pipeline_mode must be staged or concurrent")
        if self.decode_workers < 1:
            raise PipelineError("decode.queue.decode_workers must be at least 1")
        if self.pipeline_mode == "staged" and self.decode_workers != 1:
            raise PipelineError("staged mode supports exactly one decode worker")
        if self.pipeline_mode == "concurrent" and self.decode_workers not in {1, 2}:
            raise PipelineError("concurrent mode currently supports decode_workers 1 or 2")
        if self.decode_worker_mode not in {"per_chunk_command", "persistent_receiver_process"}:
            raise PipelineError(f"unsupported decode worker mode: {self.decode_worker_mode}")
        if self.output_write_strategy not in {"ordered_append", "ordered_chunk_merge"}:
            raise PipelineError(f"unsupported output write strategy: {self.output_write_strategy}")
        if self.persistent_decode_enabled and self.pipeline_mode != "concurrent":
            raise PipelineError("persistent receiver decode worker requires concurrent pipeline mode")
        if self.persistent_decode_enabled and self.decode_workers != 1:
            raise PipelineError("persistent receiver decode worker currently preserves the single-worker path only")
        if self.persistent_decode_enabled and self.queue_capacity_chunks is not None:
            if self.persistent_decode_batch_chunks > int(self.queue_capacity_chunks):
                raise PipelineError("decode.queue.persistent_batch_chunks must not exceed the bounded queue capacity")
        queue_config = self.config.get("decode", {}).get("queue", {})
        if self.pipeline_mode == "concurrent" and self.queue_capacity_chunks is not None and self.queue_capacity_chunks <= 0:
            raise PipelineError("decode.queue.capacity_chunks must be positive when provided")
        if self.pipeline_mode == "concurrent" and queue_config.get("require_bounded_queues") and self.queue_capacity_chunks is None:
            raise PipelineError("decode.queue.require_bounded_queues requires decode.queue.capacity_chunks")
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
        self.validate_experiment_contract()

    def validate_experiment_contract(self) -> None:
        experiment = self.config.get("experiment") or {}
        if not isinstance(experiment, dict) or not experiment:
            return

        def require_int_match(experiment_key: str, actual: Any, configured_name: str) -> None:
            expected = experiment.get(experiment_key)
            if expected in (None, ""):
                return
            try:
                expected_int = int(expected)
                actual_int = int(actual)
            except (TypeError, ValueError) as error:
                raise PipelineError(
                    f"experiment.{experiment_key}={expected!r} cannot be compared with {configured_name}={actual!r}"
                ) from error
            if expected_int != actual_int:
                raise PipelineError(
                    f"experiment.{experiment_key}={expected_int} does not match {configured_name}={actual_int}"
                )

        require_int_match("decoder_workers", self.decode_workers, "decode.queue.decode_workers")
        require_int_match("chunk_packets", self.chunk_packets, "decode.chunking.chunk_packets")
        require_int_match("queue_capacity_chunks", self.queue_capacity_chunks, "decode.queue.capacity_chunks")
        if self.source_mode == "pcap_replay":
            replay = self.config.get("pcap_replay", {})
            require_int_match("packets_per_repeat", replay.get("packet_limit"), "pcap_replay.packet_limit")
            require_int_match("input_rate_pps", replay.get("pps"), "pcap_replay.pps")
            expected_input = experiment.get("event_input")
            configured_input = replay.get("sender_pcap")
            if expected_input not in (None, "") and str(expected_input) != str(configured_input):
                raise PipelineError(
                    f"experiment.event_input={expected_input!r} does not match pcap_replay.sender_pcap={configured_input!r}"
                )
        expected_bounded = experiment.get("bounded_queues")
        if expected_bounded is True and not (self.pipeline_mode == "concurrent" and self.queue_capacity_chunks):
            raise PipelineError("experiment.bounded_queues requires concurrent mode with decode.queue.capacity_chunks")
        expected_output_root = experiment.get("output_root")
        configured_output_root = self.config.get("paths", {}).get("output_root") or "/tmp/iceberg_fabric_runs"
        if expected_output_root not in (None, "") and str(expected_output_root).rstrip("/") != str(configured_output_root).rstrip("/"):
            raise PipelineError(
                f"experiment.output_root={expected_output_root!r} does not match paths.output_root={configured_output_root!r}"
            )
        expected_reference = experiment.get("validation_reference_csv")
        configured_reference = self.config.get("validation", {}).get("reference_csv")
        if expected_reference not in (None, "") and str(expected_reference) != str(configured_reference):
            raise PipelineError(
                "experiment.validation_reference_csv does not match validation.reference_csv"
            )

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
            "manifest_type": "iceberg_fabric_unified_manifest",
            "schema_version": 1,
            "architecture_mode": (
                "queue_v2_concurrent_adaptive_experimental"
                if self.pipeline_mode == "concurrent" and decode.get("adaptive", {}).get("enabled")
                else "queue_v2_concurrent_fixed"
                if self.pipeline_mode == "concurrent"
                else "queue_v2_adaptive_experimental"
                if decode.get("adaptive", {}).get("enabled")
                else "queue_v2_fixed"
            ),
            "pipeline_mode": self.pipeline_mode,
            "decode_worker_mode": self.decode_worker_mode,
            "status": status,
            "phase": phase,
            "updated_at": now_iso(),
            "run_id": self.layout.run_id,
            "source_mode": self.source_mode,
            "label": self.label,
            "experiment": self.config.get("experiment") or {},
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
                "queue": decode.get("queue", {}),
            },
            "output_writer": {
                "write_strategy": self.output_write_strategy,
                "ordered_output": True,
                "shared_csv_multi_writer": False,
                "description": (
                    "decoded chunks are merged once in deterministic chunk_index order"
                    if self.output_write_strategy == "ordered_chunk_merge"
                    else "single coordinator appends decoded chunks to combined.csv strictly by chunk_index"
                ),
            },
            "paths": {
                "local_run_dir": str(self.layout.local_run_dir),
                "local_manifest": str(self.layout.manifest_json),
                "local_summary": str(self.layout.summary_json),
                "local_queue_history_csv": str(self.layout.queue_history_csv),
                "local_measurement_csv": str(self.layout.measurement_csv),
                "local_validation_json": str(self.layout.validation_json),
                "local_report_mirror_dir": str(self.layout.report_mirror_dir) if self.layout.report_mirror_dir else None,
                "local_combined_csv": str(self.layout.local_combined_csv),
                "local_decoded_chunk_dir": str(self.layout.local_decoded_chunk_dir),
                "local_visualization_dir": str(self.layout.viz_dir),
                "remote_raw_chunk_dir": self.layout.remote_raw_chunk_dir,
                "remote_decoded_chunk_dir": self.layout.remote_decoded_chunk_dir,
                "remote_combined_csv": self.layout.remote_combined_csv,
                "remote_manifest": self.layout.remote_manifest_json,
                "remote_summary": self.layout.remote_summary_json,
                "remote_helper_dir": self.layout.remote_helper_dir,
                "remote_validator": self.layout.remote_validator,
                "remote_decode_worker": self.layout.remote_decode_worker,
                "remote_decode_task_dir": self.layout.remote_decode_task_dir,
                "remote_decode_status_dir": self.layout.remote_decode_status_dir,
                "remote_decode_worker_dir": self.layout.remote_decode_worker_dir,
            },
            "validation": {
                "enabled": self.validation_enabled,
                "reference_csv": validation.get("reference_csv") or None,
                "compare_fields": validation.get("compare_fields") or None,
                "expected_packets": validation.get("expected_packets"),
                "expected_rows": validation.get("expected_rows"),
            },
            "queue_metrics": self.queue_metrics(),
            "persistent_decode_batches": [],
            "persistent_decode_result_records": [],
            "ordered_output_records": [],
            "chunk_delivery_accounting": self.chunk_delivery_accounting(),
            "decode_worker_assignments": [],
            "raw_chunks": [],
            "chunks": [],
        }

    def queue_metrics(self) -> dict[str, Any]:
        with self._state_lock:
            raw_count = len([chunk for chunk in self.chunks if chunk.get("raw_chunk_preserved")])
            decoded_count = len([chunk for chunk in self.chunks if chunk.get("decode_status") == "decoded"])
            appended_count = len([chunk for chunk in self.chunks if chunk.get("append_status") == "appended"])
            drops = sum(int(chunk.get("tcpdump_dropped_by_kernel") or 0) for chunk in self.chunks)
            queue_depth = max(0, raw_count - decoded_count)
            append_backlog = max(0, decoded_count - appended_count)
            self.max_queue_depth_chunks = max(self.max_queue_depth_chunks, queue_depth)
            returned_to_zero = raw_count > 0 and decoded_count == raw_count and queue_depth == 0
            raw_backpressure = self.backpressure_metrics.get("raw_to_decode", {})
            writer_backpressure = self.backpressure_metrics.get("decoded_to_writer", {})
            bounded_queue_names = [
                name
                for name, stats in self.backpressure_metrics.items()
                if stats.get("bounded")
            ]
            return {
                "pipeline_mode": self.pipeline_mode,
                "decode_worker_mode": self.decode_worker_mode,
                "raw_chunks_produced": raw_count,
                "decoded_chunks_completed": decoded_count,
                "raw_chunk_preserved_count": raw_count,
                "decode_completed_chunk_count": decoded_count,
                "append_completed_chunk_count": appended_count,
                "queue_depth_chunks": queue_depth,
                "final_queue_depth_chunks": queue_depth,
                "max_queue_depth_chunks": self.max_queue_depth_chunks,
                "append_backlog_chunks": append_backlog,
                "queue_returned_to_zero": returned_to_zero,
                "raw_packet_drops": drops,
                "decode_workers_configured": self.decode_workers,
                "queue_capacity_chunks": self.queue_capacity_chunks,
                "bounded_queues": self.pipeline_mode == "concurrent" and bool(self.queue_capacity_chunks),
                "bounded_queue_names": bounded_queue_names,
                "output_write_strategy": self.output_write_strategy,
                "raw_to_decode_queue_depth_chunks": raw_backpressure.get("current_depth"),
                "decoded_to_writer_queue_depth_chunks": writer_backpressure.get("current_depth"),
                "writer_queue_depth_chunks": writer_backpressure.get("current_depth"),
                "raw_to_decode_max_depth_after_put_chunks": raw_backpressure.get("max_depth_after_put"),
                "decoded_to_writer_max_depth_after_put_chunks": writer_backpressure.get("max_depth_after_put"),
                "writer_queue_max_depth_after_put_chunks": writer_backpressure.get("max_depth_after_put"),
                "writer_queue_blocked_put_count": writer_backpressure.get("blocked_put_count"),
                "writer_queue_blocking_seconds": writer_backpressure.get("total_blocked_put_seconds"),
                "max_ordered_write_buffer_depth_chunks": self.max_ordered_write_buffer_depth_chunks,
                "backpressure": copy.deepcopy(self.backpressure_metrics),
            }

    def chunk_delivery_accounting(self) -> dict[str, Any]:
        with self._state_lock:
            expected_chunk_ids = sorted(
                int(chunk["chunk_index"])
                for chunk in self.chunks
                if chunk.get("chunk_index") not in (None, "")
            )
            submitted_chunk_ids: list[int] = []
            for batch in getattr(self, "persistent_decode_batches", []):
                for chunk_id in batch.get("chunk_ids") or batch.get("chunk_indices") or []:
                    submitted_chunk_ids.append(int(chunk_id))
            if not submitted_chunk_ids:
                submitted_chunk_ids = [
                    int(item["chunk_index"])
                    for item in self.decode_worker_assignments
                    if item.get("chunk_index") not in (None, "")
                ]
            result_chunk_ids = [
                int(item["chunk_index"])
                for item in getattr(self, "persistent_decode_result_records", [])
                if item.get("chunk_index") not in (None, "")
            ]
            if not result_chunk_ids:
                result_chunk_ids = [
                    int(item["chunk_index"])
                    for item in self.decode_worker_assignments
                    if item.get("status") == "decoded" and item.get("chunk_index") not in (None, "")
                ]
            ordered_output_chunk_ids = [
                int(item["chunk_index"])
                for item in getattr(self, "ordered_output_records", [])
                if item.get("chunk_index") not in (None, "")
            ]
            if not ordered_output_chunk_ids:
                ordered_output_chunk_ids = [
                    int(chunk["chunk_index"])
                    for chunk in self.chunks
                    if chunk.get("append_status") == "appended" and chunk.get("chunk_index") not in (None, "")
                ]

        submitted = chunk_once_accounting(expected_chunk_ids, submitted_chunk_ids)
        returned = chunk_once_accounting(expected_chunk_ids, result_chunk_ids)
        ordered = chunk_once_accounting(expected_chunk_ids, ordered_output_chunk_ids)
        return {
            "expected_chunk_ids": expected_chunk_ids,
            "submitted": submitted,
            "results_returned": returned,
            "ordered_output": ordered,
            "every_chunk_submitted_exactly_once": submitted["every_expected_chunk_exactly_once"],
            "every_result_returned_exactly_once": returned["every_expected_chunk_exactly_once"],
            "ordered_output_includes_every_chunk_exactly_once": ordered["every_expected_chunk_exactly_once"],
        }


    def record_queue_metrics(self, *, status: str, phase: str, metrics: dict[str, Any]) -> None:
        sample = {
            "updated_at": now_iso(),
            "status": status,
            "phase": phase,
            "raw_chunks_produced": metrics.get("raw_chunks_produced"),
            "decoded_chunks_completed": metrics.get("decoded_chunks_completed"),
            "append_completed_chunk_count": metrics.get("append_completed_chunk_count"),
            "queue_depth_chunks": metrics.get("queue_depth_chunks"),
            "final_queue_depth_chunks": metrics.get("final_queue_depth_chunks"),
            "max_queue_depth_chunks": metrics.get("max_queue_depth_chunks"),
            "append_backlog_chunks": metrics.get("append_backlog_chunks"),
            "queue_returned_to_zero": metrics.get("queue_returned_to_zero"),
            "raw_packet_drops": metrics.get("raw_packet_drops"),
            "decode_workers_configured": metrics.get("decode_workers_configured"),
            "queue_capacity_chunks": metrics.get("queue_capacity_chunks"),
            "bounded_queues": metrics.get("bounded_queues"),
            "raw_to_decode_queue_depth_chunks": metrics.get("raw_to_decode_queue_depth_chunks"),
            "decoded_to_writer_queue_depth_chunks": metrics.get("decoded_to_writer_queue_depth_chunks"),
            "writer_queue_depth_chunks": metrics.get("writer_queue_depth_chunks"),
            "writer_queue_max_depth_after_put_chunks": metrics.get("writer_queue_max_depth_after_put_chunks"),
            "writer_queue_blocked_put_count": metrics.get("writer_queue_blocked_put_count"),
            "writer_queue_blocking_seconds": metrics.get("writer_queue_blocking_seconds"),
            "backpressure_blocked_put_count": sum(
                int(channel.get("blocked_put_count") or 0)
                for channel in (metrics.get("backpressure") or {}).values()
            ),
            "backpressure_blocking_seconds": round(
                sum(
                    float(channel.get("total_blocked_put_seconds") or 0.0)
                    for channel in (metrics.get("backpressure") or {}).values()
                ),
                6,
            ),
        }
        self.queue_metrics_history.append(sample)
        if len(self.queue_metrics_history) > 1000:
            self.queue_metrics_history = self.queue_metrics_history[-1000:]

    def record_queue_metrics_snapshot(self, *, status: str, phase: str) -> None:
        with self._state_lock:
            metrics = self.queue_metrics()
            self.record_queue_metrics(status=status, phase=phase, metrics=metrics)

    def write_manifest(self, *, status: str, phase: str, error: str | None = None) -> None:
        with self._state_lock:
            metrics = self.queue_metrics()
            self.record_queue_metrics(status=status, phase=phase, metrics=metrics)
            self.manifest.update(
                {
                    "status": status,
                    "phase": phase,
                    "updated_at": now_iso(),
                    "pipeline_mode": self.pipeline_mode,
                    "decode_worker_mode": self.decode_worker_mode,
                    "queue_metrics": metrics,
                    "persistent_decode_workers": copy.deepcopy(self.persistent_decode_workers),
                    "persistent_decode_worker_failures": copy.deepcopy(self.persistent_decode_worker_failures),
                    "persistent_decode_batches": copy.deepcopy(self.persistent_decode_batches),
                    "persistent_decode_result_records": copy.deepcopy(getattr(self, "persistent_decode_result_records", [])),
                    "persistent_decode_fallback_used": self.persistent_decode_fallback_used,
                    "output_merge_records": copy.deepcopy(self.output_merge_records),
                    "ordered_output_records": copy.deepcopy(getattr(self, "ordered_output_records", [])),
                    "chunk_delivery_accounting": self.chunk_delivery_accounting(),
                    "queue_metrics_history": list(self.queue_metrics_history),
                    "decode_worker_assignments": list(self.decode_worker_assignments),
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
        validation = {
            "enabled": self.validation_enabled,
            "pass": None,
            "reason": "dry-run: validation not executed",
            "validation_started_at": None,
            "validation_completed_at": None,
            "validation_elapsed_seconds": 0.0,
            "validation_time_seconds": 0.0,
            "dry_run_placeholder": True,
        }
        measurement_summary = self.build_dry_run_measurement_summary(validation=validation)
        pass_criteria = self.build_pass_criteria(measurement_summary)
        measurement_summary["pass_criteria"] = pass_criteria
        measurement_summary["failure_point"] = pass_criteria["failure_point"]
        self.manifest["validation_result"] = validation
        self.manifest["measurement_summary"] = measurement_summary
        self.manifest["pass_criteria"] = pass_criteria
        self.manifest["failure_point"] = pass_criteria["failure_point"]
        self.manifest["pass"] = None
        self.write_manifest(status="dry_run", phase="planned")
        payload = {
            "dry_run": True,
            "pass": None,
            "failure_point": pass_criteria["failure_point"],
            "pass_criteria": pass_criteria,
            "run_id": self.layout.run_id,
            "source_mode": self.source_mode,
            "pipeline_mode": self.pipeline_mode,
            "decode_worker_mode": self.decode_worker_mode,
            "manifest": str(self.layout.manifest_json),
            "validation": validation,
            "measurement_summary": measurement_summary,
            "measurement_csv": str(self.layout.measurement_csv),
            "queue_metrics_history_csv": str(self.layout.queue_history_csv),
            "config_path": str(self.config_path),
            "local_run_dir": str(self.layout.local_run_dir),
            "configuration": self.config,
            "transport": self.transport.describe(),
            "architecture": {
                "input_adapters": {
                    "pcap_replay": "tcpreplay on sender VM injects a config-selected PCAP into the receiver NIC",
                    "live_nic": "receiver VM captures rolling chunks from traffic already arriving on the NIC; no tcpreplay",
                },
                "shared_after_receiver_nic": self.manifest["shared_pipeline"],
                "decoded_chunk_mirroring": "deferred post-live reporting copy; decoder workers hand off remote decoded CSV paths without local copy",
            },
            "local_paths": {
                "run_dir": str(self.layout.local_run_dir),
                "manifest_json": str(self.layout.manifest_json),
                "summary_json": str(self.layout.summary_json),
                "dry_run_json": str(self.layout.dry_run_json),
                "validation_json": str(self.layout.validation_json),
                "queue_history_csv": str(self.layout.queue_history_csv),
                "measurement_csv": str(self.layout.measurement_csv),
                "report_mirror_dir": str(self.layout.report_mirror_dir) if self.layout.report_mirror_dir else None,
                "combined_csv": str(self.layout.local_combined_csv),
                "decoded_chunk_dir": str(self.layout.local_decoded_chunk_dir),
                "visualization_dir": str(self.layout.viz_dir),
            },
            "remote_paths": self.manifest["paths"],
            "planned_commands": self.planned_commands(),
            "notes": [
                "Dry-run does not contact FABRIC VMs and does not run tcpreplay.",
                "Runtime outputs default to the configured paths.output_root, or /tmp/iceberg_fabric_runs if unset.",
                f"Selected transport backend: {self.transport.transport_type}.",
                f"Queue pipeline mode: {self.pipeline_mode}.",
                f"Decode worker mode: {self.decode_worker_mode}.",
                (
                    "Concurrent mode uses a producer-consumer queue; raw chunks are decoded only after atomic rename and a ready marker."
                    if self.pipeline_mode == "concurrent"
                    else "Staged mode uses capture one chunk, decode/export, append, then continue path."
                ),
                "Decoded chunk CSV local mirroring is deferred to post-live reporting, after append/validation/live measurement.",
                "Adaptive mode is experimental and disabled unless requested in config or CLI.",
                "Required metric fields are emitted with dry-run placeholders only; they are not runtime measurements.",
            ],
        }
        self.write_report_bundle(payload)
        atomic_write_json(self.layout.dry_run_json, payload)
        mirrored = self.mirror_current_reports()
        if mirrored:
            payload["report_mirror"] = {
                "report_dir": str(self.layout.report_mirror_dir),
                "files": mirrored,
            }
            atomic_write_json(self.layout.summary_json, payload)
            atomic_write_json(self.layout.dry_run_json, payload)
            self.mirror_current_reports()
        if emit_json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        return payload

    def planned_commands(self) -> dict[str, Any]:
        common_decode = self.decode_command(0, "<RAW_CHUNK_PCAP>", "<CHUNK_CSV>")
        if self.persistent_decode_enabled:
            common_decode = "persistent receiver worker task: <RAW_CHUNK_PCAP> -> <CHUNK_CSV>"
        commands: dict[str, Any] = {
            "receiver_preflight": self.receiver_preflight_command(),
            "prepare_receiver_dirs": self.prepare_receiver_command(),
            "shared_decode_export": common_decode,
            "decode_worker_mode": self.decode_worker_mode,
            "persistent_decode_worker_start": self.persistent_decode_worker_start_command(1) if self.persistent_decode_enabled else None,
            "shared_append": self.append_command(0, "<CHUNK_CSV>"),
            "shared_validation": self.validation_command() if self.validation_enabled else None,
            "post_live_reporting": "copy remote decoded chunk CSVs to local decoded_chunks artifacts after append/validation/live measurement",
        }
        if self.source_mode == "pcap_replay":
            receiver_capture = (
                self.pcap_receiver_capture_ready_command(0, "<PACKETS>", "<RAW_CHUNK_PCAP>", "<TCPDUMP_LOG>")
                if self.pipeline_mode == "concurrent"
                else self.pcap_receiver_capture_command(0, "<PACKETS>", "<RAW_CHUNK_PCAP>", "<TCPDUMP_LOG>")
            )
            commands["input_adapter"] = {
                "split_sender_pcap": self.split_source_pcap_command(),
                "receiver_capture": receiver_capture,
                "sender_replay": self.sender_replay_command({"index": 0, "packets": "<PACKETS>", "path": "<SOURCE_CHUNK_PCAP>"}),
            }
        else:
            receiver_capture = (
                self.live_capture_ready_command(0, "<RAW_CHUNK_PCAP>", "<TCPDUMP_LOG>")
                if self.pipeline_mode == "concurrent"
                else self.live_capture_command(0, "<RAW_CHUNK_PCAP>", "<TCPDUMP_LOG>")
            )
            commands["input_adapter"] = {
                "rolling_receiver_capture": receiver_capture,
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
            f"{shlex.quote(self.layout.remote_helper_dir)} "
            f"{shlex.quote(self.layout.remote_decode_task_dir)} "
            f"{shlex.quote(self.layout.remote_decode_status_dir)} "
            f"{shlex.quote(self.layout.remote_decode_worker_dir)}"
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

    def capture_completed_marker_path(self, index: int) -> str:
        return f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_capture_completed_at.txt"

    def raw_ready_marker_path(self, index: int) -> str:
        return f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_raw.ready"

    def append_completed_marker_path(self, index: int) -> str:
        return f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_append_completed_at.txt"

    def pcap_receiver_capture_ready_command(self, index: int, packets: Any, remote_pcap: str, remote_log: str) -> str:
        partial_pcap = remote_pcap + ".partial"
        completed_marker = self.capture_completed_marker_path(index)
        ready_marker = self.raw_ready_marker_path(index)
        return (
            "set -euo pipefail; "
            f"mkdir -p {shlex.quote(self.layout.remote_receiver_run_dir)} {shlex.quote(self.layout.remote_raw_chunk_dir)}; "
            "set +e; "
            f"sudo tcpdump -i {shlex.quote(self.receiver_interface)} -n -s 0 -B 8192 -U "
            f"-c {shlex.quote(str(packets))} -w {shlex.quote(partial_pcap)} "
            f"{shlex.quote(self.capture_filter)} > {shlex.quote(remote_log)} 2>&1; "
            "rc=$?; set -e; "
            f"date -u +%Y-%m-%dT%H:%M:%SZ > {shlex.quote(completed_marker)}; "
            "if [ \"$rc\" -ne 0 ]; then exit \"$rc\"; fi; "
            f"sudo chown ubuntu:ubuntu {shlex.quote(partial_pcap)} {shlex.quote(remote_log)} 2>/dev/null || true; "
            f"mv -f {shlex.quote(partial_pcap)} {shlex.quote(remote_pcap)}; "
            f"test -s {shlex.quote(remote_pcap)}; "
            f"date -u +%Y-%m-%dT%H:%M:%SZ > {shlex.quote(ready_marker)}"
        )

    def live_capture_ready_command(self, index: int, remote_pcap: str, remote_log: str) -> str:
        seconds = float(self.config.get("live_nic", {}).get("chunk_seconds") or 5)
        partial_pcap = remote_pcap + ".partial"
        completed_marker = self.capture_completed_marker_path(index)
        ready_marker = self.raw_ready_marker_path(index)
        return (
            "set -euo pipefail; "
            f"mkdir -p {shlex.quote(self.layout.remote_receiver_run_dir)} {shlex.quote(self.layout.remote_raw_chunk_dir)}; "
            "set +e; "
            f"sudo timeout --signal=INT --kill-after=5s {seconds:g}s "
            f"tcpdump -i {shlex.quote(self.receiver_interface)} -n -s 0 -B 8192 -U "
            f"-w {shlex.quote(partial_pcap)} {shlex.quote(self.capture_filter)} "
            f"> {shlex.quote(remote_log)} 2>&1; "
            "rc=$?; set -e; "
            f"date -u +%Y-%m-%dT%H:%M:%SZ > {shlex.quote(completed_marker)}; "
            "if [ \"$rc\" -ne 0 ] && [ \"$rc\" -ne 124 ]; then exit \"$rc\"; fi; "
            f"sudo chown ubuntu:ubuntu {shlex.quote(partial_pcap)} {shlex.quote(remote_log)} 2>/dev/null || true; "
            f"mv -f {shlex.quote(partial_pcap)} {shlex.quote(remote_pcap)}; "
            f"test -s {shlex.quote(remote_pcap)}; "
            f"date -u +%Y-%m-%dT%H:%M:%SZ > {shlex.quote(ready_marker)}"
        )

    def append_with_marker_command(self, index: int, remote_csv: str) -> str:
        return self.append_command(index, remote_csv) + f"; date -u +%Y-%m-%dT%H:%M:%SZ > {shlex.quote(self.append_completed_marker_path(index))}"

    def assert_raw_chunk_ready(self, index: int, raw_pcap: str) -> None:
        ready_marker = self.raw_ready_marker_path(index)
        self.remote_exec(
            self.receiver_node,
            f"test -s {shlex.quote(raw_pcap)} && test -f {shlex.quote(ready_marker)}",
            30,
            True,
        )

    def capture_one_pcap_raw_chunk(self, chunk: dict[str, Any]) -> dict[str, Any]:
        index = int(chunk["index"])
        expected_packets = int(chunk["packets"])
        raw_pcap = f"{self.layout.remote_raw_chunk_dir}/receiver_chunk_{index:03d}.pcap"
        tcpdump_log = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_tcpdump.log"
        capture_cmd = self.pcap_receiver_capture_ready_command(index, expected_packets, raw_pcap, tcpdump_log)
        remote_pid = self.layout.remote_receiver_run_dir + f"/chunk_{index:03d}_tcpdump.pid"
        start_log = self.layout.remote_receiver_run_dir + f"/chunk_{index:03d}_tcpdump_start.log"
        start_cmd = (
            "set -euo pipefail; "
            f"nohup bash -lc {shlex.quote(capture_cmd)} </dev/null > {shlex.quote(start_log)} 2>&1 & "
            f"echo $! > {shlex.quote(remote_pid)}; "
            "sleep 1; "
            f"kill -0 $(cat {shlex.quote(remote_pid)})"
        )
        result: dict[str, Any] = {
            "chunk_index": index,
            "raw_chunk_preserved": False,
            "raw_chunk_atomic_rename": True,
            "raw_chunk_partial_path": raw_pcap + ".partial",
            "raw_chunk_pcap_path": raw_pcap,
            "raw_chunk_ready_marker_path": self.raw_ready_marker_path(index),
            "capture_completed_marker_path": self.capture_completed_marker_path(index),
            "tcpdump_log_path": tcpdump_log,
            "sender_source_pcap": chunk["path"],
            "packets_expected": expected_packets,
            "status": "capturing",
        }
        capture_start = time.monotonic()
        result["_capture_start_monotonic"] = capture_start
        result["capture_start_time"] = now_iso()
        self.remote_exec(self.receiver_node, start_cmd, 30, True)

        replay_start = time.monotonic()
        result["_replay_start_monotonic"] = replay_start
        result["replay_start_time"] = now_iso()
        sender_result = self.remote_exec(self.sender_node, self.sender_replay_command(chunk), self.timeout, False)
        replay_end = time.monotonic()
        result["_replay_end_monotonic"] = replay_end
        result["replay_end_time"] = now_iso()
        result["sender_returncode"] = sender_result.returncode
        result["replay_elapsed_seconds"] = round(replay_end - replay_start, 3)
        if sender_result.returncode != 0:
            raise PipelineError(f"chunk {index} sender replay failed: {sender_result.stderr or sender_result.stdout}")

        wait_cmd = (
            f"pid=$(cat {shlex.quote(remote_pid)}); "
            "for i in $(seq 1 30); do kill -0 \"$pid\" 2>/dev/null || exit 0; sleep 1; done; "
            "kill -INT \"$pid\" 2>/dev/null || true"
        )
        self.remote_exec(self.receiver_node, wait_cmd, 45, False)
        self.assert_raw_chunk_ready(index, raw_pcap)
        return self.publish_raw_chunk_entry(result, packets_expected=expected_packets)

    def capture_one_live_raw_chunk(self, index: int) -> dict[str, Any]:
        raw_pcap = f"{self.layout.remote_raw_chunk_dir}/receiver_chunk_{index:03d}.pcap"
        tcpdump_log = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_tcpdump.log"
        result: dict[str, Any] = {
            "chunk_index": index,
            "raw_chunk_preserved": False,
            "raw_chunk_atomic_rename": True,
            "raw_chunk_partial_path": raw_pcap + ".partial",
            "raw_chunk_pcap_path": raw_pcap,
            "raw_chunk_ready_marker_path": self.raw_ready_marker_path(index),
            "capture_completed_marker_path": self.capture_completed_marker_path(index),
            "tcpdump_log_path": tcpdump_log,
            "packets_expected": None,
            "status": "capturing",
        }
        capture_start = time.monotonic()
        result["_capture_start_monotonic"] = capture_start
        result["capture_start_time"] = now_iso()
        self.remote_exec(self.receiver_node, self.live_capture_ready_command(index, raw_pcap, tcpdump_log), self.timeout, True)
        self.assert_raw_chunk_ready(index, raw_pcap)
        return self.publish_raw_chunk_entry(result, packets_expected=None)

    def publish_raw_chunk_entry(self, result: dict[str, Any], *, packets_expected: int | None) -> dict[str, Any]:
        capture_end = time.monotonic()
        result["_capture_end_monotonic"] = capture_end
        result["capture_elapsed_seconds"] = round(capture_end - float(result["_capture_start_monotonic"]), 3)
        published_at = now_iso()
        result.update(
            {
                "raw_chunk_preserved": True,
                "status": "queued_for_decode",
                "packets_captured": None,
                "tcpdump_dropped_by_kernel": None,
                "tcpdump_statistics": {"summary_found": False, "deferred": True},
                "capture_accounting_status": "deferred",
                "capture_accounting_deferred": True,
                "capture_accounting_started_time": None,
                "capture_accounting_completed_time": None,
                "raw_chunk_published_time": published_at,
                "queued_ready_for_decode_time": published_at,
            }
        )
        with self._chunk_condition:
            self.chunks.append(result)
            self._chunk_condition.notify_all()
        self.record_queue_metrics_snapshot(status="running", phase="capture_produce")
        return result

    def collect_raw_chunk_accounting(self, chunk_entry: dict[str, Any]) -> None:
        if chunk_entry.get("capture_accounting_status") == "complete":
            return
        index = int(chunk_entry["chunk_index"])
        raw_pcap = str(chunk_entry["raw_chunk_pcap_path"])
        packets_expected = chunk_entry.get("packets_expected")
        accounting_started_at = now_iso()
        count_cmd = f"tcpdump -nn -r {shlex.quote(raw_pcap)} {shlex.quote(self.capture_filter)} 2>/dev/null | wc -l"
        count_result = self.remote_exec(self.receiver_node, count_cmd, 60, True)
        packets_captured = parse_last_int(count_result.stdout)
        tcpdump_log = str(chunk_entry["tcpdump_log_path"])
        tcpdump_text = self.remote_exec(self.receiver_node, f"cat {shlex.quote(tcpdump_log)} 2>/dev/null || true", 30, False)
        tcpdump_stats = parse_tcpdump_stats(tcpdump_text.stdout)
        capture_completed_at = self.remote_timestamp_or_none(self.receiver_node, self.capture_completed_marker_path(index))
        ready_at = self.remote_timestamp_or_none(self.receiver_node, self.raw_ready_marker_path(index)) or chunk_entry.get("queued_ready_for_decode_time") or now_iso()
        sender_statistics: dict[str, Any] | None = None
        if self.source_mode == "pcap_replay":
            sender_log = f"{self.layout.remote_sender_run_dir}/chunk_{index:03d}_tcpreplay.log"
            sender_text = self.remote_exec(self.sender_node, f"cat {shlex.quote(sender_log)} 2>/dev/null || true", 30, False)
            sender_statistics = parse_sender_stats(sender_text.stdout)
            successful = sender_statistics.get("successful_packets")
            failed = sender_statistics.get("failed_packets", 0)
            if successful is not None and packets_expected not in (None, "") and int(successful) != int(packets_expected):
                raise PipelineError(f"chunk {index} sender sent {successful} packets, expected {packets_expected}")
            if int(failed or 0) != 0:
                raise PipelineError(f"chunk {index} sender reported failed packets: {sender_statistics}")
        if packets_expected not in (None, "") and packets_captured != int(packets_expected):
            raise PipelineError(f"chunk {index} captured {packets_captured} packets, expected {packets_expected}")
        update = {
            "packets_captured": packets_captured,
            "tcpdump_dropped_by_kernel": tcpdump_stats.get("packets_dropped_by_kernel", 0),
            "tcpdump_statistics": tcpdump_stats,
            "raw_chunk_capture_complete_time": capture_completed_at,
            "capture_completed_time": capture_completed_at,
            "queued_ready_for_decode_time": ready_at,
            "capture_accounting_status": "complete",
            "capture_accounting_started_time": accounting_started_at,
            "capture_accounting_completed_time": now_iso(),
        }
        if sender_statistics is not None:
            update["sender_statistics"] = sender_statistics
        with self._chunk_condition:
            chunk_entry.update(update)
            self._chunk_condition.notify_all()

    def finalize_deferred_capture_accounting(self) -> None:
        with self._state_lock:
            deferred_chunks = [
                chunk
                for chunk in self.chunks
                if chunk.get("raw_chunk_preserved") and chunk.get("capture_accounting_status") == "deferred"
            ]
        for chunk_entry in sorted(deferred_chunks, key=lambda item: int(item.get("chunk_index", -1))):
            self.collect_raw_chunk_accounting(chunk_entry)
        if deferred_chunks:
            self.write_manifest(status="running", phase="capture_accounting_complete")

    def finish_raw_chunk_entry(self, result: dict[str, Any], *, packets_expected: int | None) -> dict[str, Any]:
        entry = self.publish_raw_chunk_entry(result, packets_expected=packets_expected)
        self.collect_raw_chunk_accounting(entry)
        self.write_manifest(status="running", phase="capture_produce")
        return entry

    def persistent_decode_worker_paths(self, worker_id: int) -> dict[str, str]:
        worker_dir = f"{self.layout.remote_decode_worker_dir}/worker_{worker_id:02d}"
        return {
            "worker_dir": worker_dir,
            "pid_file": f"{worker_dir}/worker.pid",
            "ready_file": f"{worker_dir}/worker.ready.json",
            "exit_file": f"{worker_dir}/worker.exit.json",
            "stop_file": f"{worker_dir}/worker.stop",
            "log_file": f"{worker_dir}/worker.log",
        }

    def persistent_decode_task_path(self, index: int) -> str:
        return f"{self.layout.remote_decode_task_dir}/chunk_{index:03d}.task.json"

    def persistent_decode_status_path(self, index: int) -> str:
        return f"{self.layout.remote_decode_status_dir}/chunk_{index:03d}.status.json"

    def persistent_decode_worker_start_command(self, worker_id: int) -> str:
        decode = self.config.get("decode", {})
        paths = self.persistent_decode_worker_paths(worker_id)
        worker_args = [
            "python3",
            self.layout.remote_decode_worker,
            "--task-dir",
            self.layout.remote_decode_task_dir,
            "--status-dir",
            self.layout.remote_decode_status_dir,
            "--stop-file",
            paths["stop_file"],
            "--ready-file",
            paths["ready_file"],
            "--exit-file",
            paths["exit_file"],
            "--worker-id",
            str(worker_id),
            "--mapping",
            self.mapping_csv,
            "--tpc",
            str(decode.get("tpc", 0)),
            "--plane",
            str(decode.get("plane", 0)),
        ]
        if decode.get("verbose"):
            worker_args.append("--verbose")
        worker_command = f"PYTHONPATH={shlex.quote(self.wibviz_src)} " + shell_join(worker_args)
        return (
            "set -euo pipefail; "
            f"mkdir -p {shlex.quote(paths['worker_dir'])} "
            f"{shlex.quote(self.layout.remote_decode_task_dir)} "
            f"{shlex.quote(self.layout.remote_decode_status_dir)}; "
            f"rm -f {shlex.quote(paths['stop_file'])} "
            f"{shlex.quote(paths['ready_file'])} "
            f"{shlex.quote(paths['exit_file'])}; "
            f"rm -f {shlex.quote(self.layout.remote_decode_task_dir)}/chunk_*.task.json "
            f"{shlex.quote(self.layout.remote_decode_task_dir)}/chunk_*.task.json.worker*.claimed "
            f"{shlex.quote(self.layout.remote_decode_status_dir)}/chunk_*.status.json; "
            f"nohup bash -lc {shlex.quote(worker_command)} </dev/null > {shlex.quote(paths['log_file'])} 2>&1 & "
            f"echo $! > {shlex.quote(paths['pid_file'])}; "
            "for i in $(seq 1 60); do "
            f"if test -s {shlex.quote(paths['ready_file'])}; then cat {shlex.quote(paths['ready_file'])}; exit 0; fi; "
            f"if test -s {shlex.quote(paths['exit_file'])}; then cat {shlex.quote(paths['exit_file'])}; exit 1; fi; "
            f"pid=$(cat {shlex.quote(paths['pid_file'])}); "
            "if ! kill -0 \"$pid\" 2>/dev/null; then "
            f"cat {shlex.quote(paths['log_file'])} 2>/dev/null || true; exit 1; "
            "fi; "
            "sleep 0.5; "
            "done; "
            f"cat {shlex.quote(paths['log_file'])} 2>/dev/null || true; exit 124"
        )

    def start_persistent_decode_workers(self) -> None:
        if not self.persistent_decode_enabled:
            return
        self.persistent_decode_workers = []
        for worker_id in range(1, self.decode_workers + 1):
            paths = self.persistent_decode_worker_paths(worker_id)
            started_at = now_iso()
            started = time.monotonic()
            record: dict[str, Any] = {
                "worker_id": worker_id,
                "status": "starting",
                "started_at": started_at,
                "decode_worker_mode": self.decode_worker_mode,
                **paths,
            }
            self.persistent_decode_workers.append(record)
            result = self.remote_exec(
                self.receiver_node,
                self.persistent_decode_worker_start_command(worker_id),
                90,
                False,
            )
            startup_elapsed = round(time.monotonic() - started, 3)
            record["startup_elapsed_seconds"] = startup_elapsed
            if result.returncode != 0:
                failure = {
                    "worker_id": worker_id,
                    "phase": "startup",
                    "returncode": result.returncode,
                    "stdout_tail": result.stdout[-2000:],
                    "stderr_tail": result.stderr[-2000:],
                }
                record.update({"status": "failed", "failure": failure})
                self.persistent_decode_worker_failures.append(failure)
                raise PipelineError(f"persistent decode worker {worker_id} failed to start: {failure}")
            try:
                ready = extract_json_object(result.stdout)
            except (ValueError, json.JSONDecodeError) as error:
                failure = {
                    "worker_id": worker_id,
                    "phase": "startup_ready_parse",
                    "error": str(error),
                    "stdout_tail": result.stdout[-2000:],
                }
                record.update({"status": "failed", "failure": failure})
                self.persistent_decode_worker_failures.append(failure)
                raise PipelineError(f"persistent decode worker {worker_id} ready output was not JSON") from error
            record.update({"status": "ready", "ready": ready})
        self.write_manifest(status="running", phase="persistent_decode_worker_start")

    def stop_persistent_decode_workers(self) -> None:
        if not self.persistent_decode_enabled or not self.persistent_decode_workers:
            return
        stop_errors: list[dict[str, Any]] = []
        for record in self.persistent_decode_workers:
            worker_id = int(record.get("worker_id") or 0)
            if record.get("status") in {"stopped", "failed_to_stop"}:
                continue
            paths = self.persistent_decode_worker_paths(worker_id)
            command = (
                "set -euo pipefail; "
                f"touch {shlex.quote(paths['stop_file'])}; "
                f"pid=$(cat {shlex.quote(paths['pid_file'])} 2>/dev/null || true); "
                "if [ -z \"$pid\" ]; then exit 0; fi; "
                "for i in $(seq 1 60); do "
                "if ! kill -0 \"$pid\" 2>/dev/null; then break; fi; "
                "sleep 0.5; "
                "done; "
                "if kill -0 \"$pid\" 2>/dev/null; then kill -TERM \"$pid\" 2>/dev/null || true; sleep 1; fi; "
                "if kill -0 \"$pid\" 2>/dev/null; then exit 124; fi; "
                f"cat {shlex.quote(paths['exit_file'])} 2>/dev/null || true"
            )
            result = self.remote_exec(self.receiver_node, command, 90, False)
            if result.returncode != 0:
                failure = {
                    "worker_id": worker_id,
                    "phase": "stop",
                    "returncode": result.returncode,
                    "stdout_tail": result.stdout[-2000:],
                    "stderr_tail": result.stderr[-2000:],
                }
                record.update({"status": "failed_to_stop", "failure": failure})
                self.persistent_decode_worker_failures.append(failure)
                stop_errors.append(failure)
                continue
            exit_payload = None
            if result.stdout.strip():
                try:
                    exit_payload = extract_json_object(result.stdout)
                except (ValueError, json.JSONDecodeError):
                    exit_payload = {"raw_stdout": result.stdout.strip()}
            record.update(
                {
                    "status": "stopped",
                    "stopped_at": now_iso(),
                    "exit": exit_payload,
                }
            )
        self.write_manifest(status="running", phase="persistent_decode_worker_stop")
        if stop_errors:
            raise PipelineError(f"persistent decode worker stop failed: {stop_errors[0]}")

    def write_remote_json_atomic_shell(self, node: str, remote_path: str, payload: dict[str, Any], timeout: int = 30) -> None:
        encoded = base64.b64encode((json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")).decode("ascii")
        tmp_path = f"{remote_path}.tmp-{os.getpid()}-{time.time_ns()}"
        command = (
            "set -euo pipefail; "
            f"mkdir -p {shlex.quote(str(Path(remote_path).parent))}; "
            f"printf %s {shlex.quote(encoded)} | base64 -d > {shlex.quote(tmp_path)}; "
            f"mv -f {shlex.quote(tmp_path)} {shlex.quote(remote_path)}"
        )
        self.remote_exec(node, command, timeout, True)

    def write_remote_json_batch_atomic_shell(
        self,
        node: str,
        payloads: list[tuple[str, dict[str, Any]]],
        timeout: int = 30,
    ) -> None:
        items = [{"path": path, "payload": payload} for path, payload in payloads]
        encoded = base64.b64encode(json.dumps(items, sort_keys=True).encode("utf-8")).decode("ascii")
        script = (
            "import base64,json,os,pathlib,sys,time; "
            "items=json.loads(base64.b64decode(sys.argv[1]).decode('utf-8')); "
            "pid=os.getpid(); "
            "\nfor item in items:\n"
            "    path=pathlib.Path(item['path']); path.parent.mkdir(parents=True, exist_ok=True)\n"
            "    tmp=path.with_suffix(path.suffix + '.tmp-' + str(pid) + '-' + str(time.time_ns()))\n"
            "    tmp.write_text(json.dumps(item['payload'], indent=2, sort_keys=True) + '\\n', encoding='utf-8')\n"
            "    tmp.replace(path)\n"
        )
        self.remote_exec(node, shell_join(["python3", "-c", script, encoded]), timeout, True)

    def wait_for_persistent_decode_batch_status_command(
        self,
        *,
        status_paths: list[str],
        worker_id: int,
        batch_id: str,
    ) -> str:
        paths = self.persistent_decode_worker_paths(worker_id)
        timeout = max(1, self.timeout + 60)
        script = '''
import json
import pathlib
import sys
import time

status_paths = json.loads(sys.argv[1])
exit_file = pathlib.Path(sys.argv[2])
timeout = float(sys.argv[3])
batch_id = sys.argv[4]
started = time.monotonic()
while True:
    ready = True
    for path in status_paths:
        status_path = pathlib.Path(path)
        if not status_path.is_file() or status_path.stat().st_size <= 0:
            ready = False
            break
    if ready:
        statuses = [json.loads(pathlib.Path(path).read_text(encoding="utf-8")) for path in status_paths]
        print(json.dumps({"status": "complete", "batch_id": batch_id, "statuses": statuses}, sort_keys=True))
        sys.exit(0)
    if exit_file.is_file():
        print(exit_file.read_text(encoding="utf-8"))
        sys.exit(2)
    if time.monotonic() - started > timeout:
        print(json.dumps({"status": "timeout", "batch_id": batch_id, "status_paths": status_paths}, sort_keys=True))
        sys.exit(124)
    time.sleep(0.2)
'''
        return shell_join(["python3", "-c", script, json.dumps(status_paths), paths["exit_file"], str(timeout), batch_id])

    def wait_for_persistent_decode_status_command(self, status_path: str, worker_id: int) -> str:
        paths = self.persistent_decode_worker_paths(worker_id)
        polls = max(1, int((self.timeout + 30) / 0.2))
        timeout_payload = json.dumps(
            {
                "status": "timeout",
                "worker_id": worker_id,
                "status_path": status_path,
                "decode_worker_mode": self.decode_worker_mode,
            },
            separators=(",", ":"),
        )
        return (
            "set -euo pipefail; "
            f"for i in $(seq 1 {polls}); do "
            f"if test -s {shlex.quote(status_path)}; then cat {shlex.quote(status_path)}; exit 0; fi; "
            f"if test -s {shlex.quote(paths['exit_file'])}; then cat {shlex.quote(paths['exit_file'])}; exit 2; fi; "
            "sleep 0.2; "
            "done; "
            f"printf %s {shlex.quote(timeout_payload)}; exit 124"
        )

    def decode_with_persistent_worker(
        self,
        *,
        chunk_entry: dict[str, Any],
        worker_id: int,
        raw_pcap: str,
        remote_csv: str,
        decode_log: str,
        decode_started_marker: str,
        decode_completed_marker: str,
    ) -> dict[str, Any]:
        index = int(chunk_entry["chunk_index"])
        task_path = self.persistent_decode_task_path(index)
        status_path = self.persistent_decode_status_path(index)
        task = {
            "schema_version": 1,
            "command": "decode_chunk",
            "decode_worker_mode": self.decode_worker_mode,
            "worker_id": worker_id,
            "chunk_index": index,
            "raw_pcap": raw_pcap,
            "output_csv": remote_csv,
            "status_path": status_path,
            "decode_log": decode_log,
            "decode_started_marker": decode_started_marker,
            "decode_completed_marker": decode_completed_marker,
            "expected_packets": chunk_entry.get("packets_expected"),
            "mapping": self.mapping_csv,
            "tpc": self.config.get("decode", {}).get("tpc", 0),
            "plane": self.config.get("decode", {}).get("plane", 0),
            "track_offsets": True,
            "sort_packets": True,
            "packet_label": raw_pcap,
            "created_at": now_iso(),
        }
        submit_start = time.monotonic()
        self.write_remote_json_atomic_shell(self.receiver_node, task_path, task, 30)
        submit_elapsed = round(time.monotonic() - submit_start, 6)
        wait_start = time.monotonic()
        result = self.remote_exec(
            self.receiver_node,
            self.wait_for_persistent_decode_status_command(status_path, worker_id),
            self.timeout + 60,
            False,
        )
        wait_elapsed = round(time.monotonic() - wait_start, 6)
        if result.returncode != 0:
            failure = {
                "worker_id": worker_id,
                "chunk_index": index,
                "phase": "decode_status_wait",
                "returncode": result.returncode,
                "stdout_tail": result.stdout[-2000:],
                "stderr_tail": result.stderr[-2000:],
                "task_path": task_path,
                "status_path": status_path,
            }
            self.persistent_decode_worker_failures.append(failure)
            raise PipelineError(f"persistent decode worker failed waiting for chunk {index}: {failure}")
        try:
            status = extract_json_object(result.stdout)
        except (ValueError, json.JSONDecodeError) as error:
            failure = {
                "worker_id": worker_id,
                "chunk_index": index,
                "phase": "decode_status_parse",
                "error": str(error),
                "stdout_tail": result.stdout[-2000:],
            }
            self.persistent_decode_worker_failures.append(failure)
            raise PipelineError(f"persistent decode worker status for chunk {index} was not JSON") from error
        status.update(
            {
                "task_path": task_path,
                "status_path": status_path,
                "task_submit_seconds": submit_elapsed,
                "response_wait_seconds": wait_elapsed,
                "request_response_seconds": round(submit_elapsed + wait_elapsed, 6),
            }
        )
        if status.get("status") != "complete":
            failure = {
                "worker_id": worker_id,
                "chunk_index": index,
                "phase": "decode_task",
                "status": status.get("status"),
                "error": status.get("error"),
                "status_path": status_path,
            }
            self.persistent_decode_worker_failures.append(failure)
            raise PipelineError(f"persistent decode worker failed chunk {index}: {failure}")
        return status

    def decode_with_persistent_worker_batch(
        self,
        *,
        prepared_chunks: list[dict[str, Any]],
        worker_id: int,
        batch_id: str,
        batch_collection: dict[str, Any] | None = None,
    ) -> dict[int, dict[str, Any]]:
        payloads: list[tuple[str, dict[str, Any]]] = []
        status_paths: list[str] = []
        batch_size = len(prepared_chunks)
        for batch_index, item in enumerate(prepared_chunks):
            chunk_entry = item["chunk_entry"]
            index = int(chunk_entry["chunk_index"])
            task_path = self.persistent_decode_task_path(index)
            status_path = self.persistent_decode_status_path(index)
            item["task_path"] = task_path
            item["status_path"] = status_path
            status_paths.append(status_path)
            payloads.append(
                (
                    task_path,
                    {
                        "schema_version": 1,
                        "command": "decode_chunk",
                        "decode_worker_mode": self.decode_worker_mode,
                        "worker_id": worker_id,
                        "chunk_index": index,
                        "batch_id": batch_id,
                        "batch_size": batch_size,
                        "batch_index_in_batch": batch_index,
                        "raw_pcap": item["raw_pcap"],
                        "output_csv": item["remote_csv"],
                        "status_path": status_path,
                        "decode_log": item["decode_log"],
                        "decode_started_marker": item["decode_started_marker"],
                        "decode_completed_marker": item["decode_completed_marker"],
                        "expected_packets": chunk_entry.get("packets_expected"),
                        "mapping": self.mapping_csv,
                        "tpc": self.config.get("decode", {}).get("tpc", 0),
                        "plane": self.config.get("decode", {}).get("plane", 0),
                        "track_offsets": True,
                        "sort_packets": True,
                        "packet_label": item["raw_pcap"],
                        "created_at": now_iso(),
                    },
                )
            )

        decoder_request_started_at = now_iso()
        submit_start = time.monotonic()
        self.write_remote_json_batch_atomic_shell(self.receiver_node, payloads, 30)
        submit_elapsed = round(time.monotonic() - submit_start, 6)
        wait_start = time.monotonic()
        result = self.remote_exec(
            self.receiver_node,
            self.wait_for_persistent_decode_batch_status_command(
                status_paths=status_paths,
                worker_id=worker_id,
                batch_id=batch_id,
            ),
            self.timeout + 90,
            False,
        )
        wait_elapsed = round(time.monotonic() - wait_start, 6)
        decoder_request_completed_at = now_iso()
        batch_request_response = round(submit_elapsed + wait_elapsed, 6)
        if result.returncode != 0:
            failure = {
                "worker_id": worker_id,
                "batch_id": batch_id,
                "phase": "decode_batch_status_wait",
                "returncode": result.returncode,
                "stdout_tail": result.stdout[-2000:],
                "stderr_tail": result.stderr[-2000:],
                "status_paths": status_paths,
            }
            self.persistent_decode_worker_failures.append(failure)
            raise PipelineError(f"persistent decode worker failed waiting for batch {batch_id}: {failure}")
        try:
            batch_status = extract_json_object(result.stdout)
        except (ValueError, json.JSONDecodeError) as error:
            failure = {
                "worker_id": worker_id,
                "batch_id": batch_id,
                "phase": "decode_batch_status_parse",
                "error": str(error),
                "stdout_tail": result.stdout[-2000:],
            }
            self.persistent_decode_worker_failures.append(failure)
            raise PipelineError(f"persistent decode worker batch status {batch_id} was not JSON") from error

        statuses = list(batch_status.get("statuses") or [])
        if len(statuses) != batch_size:
            failure = {
                "worker_id": worker_id,
                "batch_id": batch_id,
                "phase": "decode_batch_status_count",
                "expected_statuses": batch_size,
                "actual_statuses": len(statuses),
                "status_paths": status_paths,
            }
            self.persistent_decode_worker_failures.append(failure)
            raise PipelineError(f"persistent decode worker batch {batch_id} returned incomplete statuses: {failure}")
        expected_chunk_ids = [int(item["chunk_entry"]["chunk_index"]) for item in prepared_chunks]
        status_chunk_ids = [int(status.get("chunk_index")) for status in statuses]
        if Counter(status_chunk_ids) != Counter(expected_chunk_ids):
            failure = {
                "worker_id": worker_id,
                "batch_id": batch_id,
                "phase": "decode_batch_status_identity",
                "expected_chunk_ids": expected_chunk_ids,
                "actual_chunk_ids": status_chunk_ids,
            }
            self.persistent_decode_worker_failures.append(failure)
            raise PipelineError(f"persistent decode worker batch {batch_id} returned wrong chunk IDs: {failure}")

        worker_processing_total = round(
            sum(float(status.get("worker_processing_seconds") or 0.0) for status in statuses),
            6,
        )
        collection = dict(batch_collection or {})
        requested_batch_size = int(collection.get("requested_batch_size_chunks") or collection.get("configured_batch_limit") or self.persistent_decode_batch_chunks)
        partial_batch = batch_size < requested_batch_size
        batch_record = {
            "batch_id": batch_id,
            "worker_id": worker_id,
            "requested_batch_size_chunks": requested_batch_size,
            "actual_batch_size_chunks": batch_size,
            "batch_size": batch_size,
            "actual_batch_size": batch_size,
            "singleton_batch": batch_size == 1,
            "partial_batch": partial_batch,
            "multi_chunk_batch": batch_size > 1,
            "chunk_ids": status_chunk_ids,
            "chunk_indices": status_chunk_ids,
            "batch_formation_started_at": collection.get("batch_formation_started_at"),
            "batch_formation_completed_at": collection.get("batch_formation_completed_at"),
            "batch_formation_timestamp": collection.get("batch_formation_timestamp"),
            "decoder_request_started_at": decoder_request_started_at,
            "decoder_request_completed_at": decoder_request_completed_at,
            "batch_wait_time_seconds": collection.get("batch_wait_time_seconds"),
            "batch_collect_wait_seconds": collection.get("batch_collect_wait_seconds"),
            "batch_task_submit_seconds": submit_elapsed,
            "batch_response_wait_seconds": wait_elapsed,
            "batch_request_response_seconds": batch_request_response,
            "batch_worker_processing_seconds": worker_processing_total,
            "decode_export_duration_seconds": worker_processing_total,
            "per_batch_decode_export_duration_seconds": worker_processing_total,
            "queue_depth_after_first_get": collection.get("queue_depth_after_first_get"),
            "queue_ready_drained_count": collection.get("queue_ready_drained_count"),
            "queue_depth_after_drain": collection.get("queue_depth_after_drain"),
            "configured_batch_limit": collection.get("configured_batch_limit", self.persistent_decode_batch_chunks),
            "collect_timeout_seconds": collection.get(
                "collect_timeout_seconds",
                self.persistent_decode_batch_collect_timeout_seconds,
            ),
            "flush_reason": collection.get("flush_reason"),
            "end_of_input_seen": collection.get("end_of_input_seen"),
            "status": "complete",
            "completed_at": decoder_request_completed_at,
        }
        result_records = [
            {
                "batch_id": batch_id,
                "worker_id": worker_id,
                "chunk_index": int(status.get("chunk_index")),
                "status": status.get("status"),
                "decode_completed_at": status.get("decode_completed_at"),
                "requested_batch_size_chunks": requested_batch_size,
                "actual_batch_size_chunks": batch_size,
            }
            for status in statuses
        ]
        with self._state_lock:
            self.persistent_decode_batches.append(batch_record)
            self.persistent_decode_result_records.extend(result_records)

        per_chunk_submit = round(submit_elapsed / batch_size, 6)
        per_chunk_wait = round(wait_elapsed / batch_size, 6)
        per_chunk_request_response = round(batch_request_response / batch_size, 6)
        by_index: dict[int, dict[str, Any]] = {}
        for status in statuses:
            index = int(status.get("chunk_index"))
            status.update(
                {
                    "batch_id": batch_id,
                    "batch_size": batch_size,
                    "requested_batch_size_chunks": requested_batch_size,
                    "actual_batch_size": batch_size,
                    "actual_batch_size_chunks": batch_size,
                    "singleton_batch": batch_size == 1,
                    "partial_batch": partial_batch,
                    "multi_chunk_batch": batch_size > 1,
                    "chunk_ids": status_chunk_ids,
                    "batch_formation_started_at": collection.get("batch_formation_started_at"),
                    "batch_formation_completed_at": collection.get("batch_formation_completed_at"),
                    "batch_formation_timestamp": collection.get("batch_formation_timestamp"),
                    "decoder_request_started_at": decoder_request_started_at,
                    "decoder_request_completed_at": decoder_request_completed_at,
                    "batch_wait_time_seconds": collection.get("batch_wait_time_seconds"),
                    "batch_collect_wait_seconds": collection.get("batch_collect_wait_seconds"),
                    "batch_task_submit_seconds": submit_elapsed,
                    "batch_response_wait_seconds": wait_elapsed,
                    "batch_request_response_seconds": batch_request_response,
                    "batch_worker_processing_seconds": worker_processing_total,
                    "decode_export_duration_seconds": worker_processing_total,
                    "per_batch_decode_export_duration_seconds": worker_processing_total,
                    "flush_reason": collection.get("flush_reason"),
                    "queue_depth_after_first_get": collection.get("queue_depth_after_first_get"),
                    "queue_ready_drained_count": collection.get("queue_ready_drained_count"),
                    "queue_depth_after_drain": collection.get("queue_depth_after_drain"),
                    "task_submit_seconds": per_chunk_submit,
                    "response_wait_seconds": per_chunk_wait,
                    "request_response_seconds": per_chunk_request_response,
                }
            )
            by_index[index] = status
        return by_index

    def decode_ready_chunks(
        self,
        chunk_entries: list[dict[str, Any]],
        worker_id: int,
        batch_collection: dict[str, Any] | None = None,
    ) -> None:
        if not chunk_entries:
            return
        if not self.persistent_decode_enabled:
            for chunk_entry in chunk_entries:
                self.decode_ready_chunk(chunk_entry, worker_id=worker_id)
            return

        ordered_entries = sorted(chunk_entries, key=lambda item: int(item.get("chunk_index", -1)))
        first_index = int(ordered_entries[0]["chunk_index"])
        last_index = int(ordered_entries[-1]["chunk_index"])
        batch_id = f"batch_{first_index:03d}_{last_index:03d}_{time.time_ns()}"
        batch_size = len(ordered_entries)
        prepared_chunks: list[dict[str, Any]] = []
        for batch_index, chunk_entry in enumerate(ordered_entries):
            index = int(chunk_entry["chunk_index"])
            raw_pcap = str(chunk_entry["raw_chunk_pcap_path"])
            remote_csv = f"{self.layout.remote_decoded_chunk_dir}/chunk_{index:03d}.csv"
            decode_log = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_decode.log"
            decode_started_marker = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_decode_started_at.txt"
            decode_completed_marker = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_decode_completed_at.txt"
            self.assert_raw_chunk_ready(index, raw_pcap)
            assigned_at = now_iso()
            with self._chunk_condition:
                assignment_index = len(self.decode_worker_assignments)
                self.decode_worker_assignments.append(
                    {
                        "worker_id": worker_id,
                        "chunk_index": index,
                        "assigned_at": assigned_at,
                        "status": "decoding",
                        "decode_worker_mode": self.decode_worker_mode,
                        "persistent_decode_batch_id": batch_id,
                        "persistent_decode_batch_size": batch_size,
                        "persistent_decode_batch_index": batch_index,
                    }
                )
                chunk_entry.update(
                    {
                        "status": "decoding",
                        "decode_status": "running",
                        "append_status": "waiting_for_decode",
                        "decode_worker_id": worker_id,
                        "decode_worker_mode": self.decode_worker_mode,
                        "decode_assignment_time": assigned_at,
                        "decoded_csv_path": remote_csv,
                        "decode_log_path": decode_log,
                        "decode_started_marker_path": decode_started_marker,
                        "decode_completed_marker_path": decode_completed_marker,
                        "persistent_decode_batch_id": batch_id,
                        "persistent_decode_batch_size": batch_size,
                        "persistent_decode_batch_index": batch_index,
                        "_decode_assignment_index": assignment_index,
                    }
                )
                self._chunk_condition.notify_all()
            prepared_chunks.append(
                {
                    "chunk_entry": chunk_entry,
                    "raw_pcap": raw_pcap,
                    "remote_csv": remote_csv,
                    "decode_log": decode_log,
                    "decode_started_marker": decode_started_marker,
                    "decode_completed_marker": decode_completed_marker,
                    "batch_index": batch_index,
                }
            )

        self.write_manifest(status="running", phase="decode_batch_submit")
        decode_start = time.monotonic()
        for chunk_entry in ordered_entries:
            chunk_entry["_decode_start_monotonic"] = decode_start
        status_by_index = self.decode_with_persistent_worker_batch(
            prepared_chunks=prepared_chunks,
            worker_id=worker_id,
            batch_id=batch_id,
            batch_collection=batch_collection,
        )
        decode_end = time.monotonic()

        for item in prepared_chunks:
            chunk_entry = item["chunk_entry"]
            index = int(chunk_entry["chunk_index"])
            persistent_result = status_by_index[index]
            stats = dict(persistent_result.get("export_stats") or {})
            expected_packets = chunk_entry.get("packets_expected")
            if expected_packets not in (None, "") and stats.get("packets_read") != int(expected_packets):
                raise PipelineError(f"chunk {index} decode read {stats.get('packets_read')} packets, expected {expected_packets}")

            local_chunk_csv = self.local_decoded_chunk_path(index)
            decode_handoff_at = now_iso()
            decode_started_at = persistent_result.get("decode_started_at")
            decode_completed_at = persistent_result.get("decode_completed_at")
            decode_elapsed = float_or_none(persistent_result.get("request_response_seconds"))
            if decode_elapsed is None:
                decode_elapsed = round(decode_end - decode_start, 3)
            persistent_fields = {
                "persistent_decode_task_path": persistent_result.get("task_path") or item.get("task_path"),
                "persistent_decode_status_path": persistent_result.get("status_path") or item.get("status_path"),
                "persistent_decode_task_submit_seconds": persistent_result.get("task_submit_seconds"),
                "persistent_decode_response_wait_seconds": persistent_result.get("response_wait_seconds"),
                "persistent_decode_request_response_seconds": persistent_result.get("request_response_seconds"),
                "persistent_decode_worker_processing_seconds": persistent_result.get("worker_processing_seconds"),
                "persistent_decode_worker_id": persistent_result.get("worker_id"),
                "persistent_decode_batch_id": persistent_result.get("batch_id"),
                "persistent_decode_batch_size": persistent_result.get("batch_size"),
                "persistent_decode_requested_batch_size_chunks": persistent_result.get("requested_batch_size_chunks"),
                "persistent_decode_actual_batch_size": persistent_result.get("actual_batch_size"),
                "persistent_decode_actual_batch_size_chunks": persistent_result.get("actual_batch_size_chunks"),
                "persistent_decode_singleton_batch": persistent_result.get("singleton_batch"),
                "persistent_decode_partial_batch": persistent_result.get("partial_batch"),
                "persistent_decode_multi_chunk_batch": persistent_result.get("multi_chunk_batch"),
                "persistent_decode_batch_index": persistent_result.get("batch_index_in_batch"),
                "persistent_decode_batch_chunk_ids": persistent_result.get("chunk_ids"),
                "persistent_decode_batch_formation_started_at": persistent_result.get("batch_formation_started_at"),
                "persistent_decode_batch_formation_completed_at": persistent_result.get("batch_formation_completed_at"),
                "persistent_decode_batch_formation_timestamp": persistent_result.get("batch_formation_timestamp"),
                "persistent_decode_decoder_request_started_at": persistent_result.get("decoder_request_started_at"),
                "persistent_decode_decoder_request_completed_at": persistent_result.get("decoder_request_completed_at"),
                "persistent_decode_batch_wait_time_seconds": persistent_result.get("batch_wait_time_seconds"),
                "persistent_decode_batch_collect_wait_seconds": persistent_result.get("batch_collect_wait_seconds"),
                "persistent_decode_batch_flush_reason": persistent_result.get("flush_reason"),
                "persistent_decode_batch_queue_depth_after_first_get": persistent_result.get("queue_depth_after_first_get"),
                "persistent_decode_batch_queue_ready_drained_count": persistent_result.get("queue_ready_drained_count"),
                "persistent_decode_batch_queue_depth_after_drain": persistent_result.get("queue_depth_after_drain"),
                "persistent_decode_batch_task_submit_seconds": persistent_result.get("batch_task_submit_seconds"),
                "persistent_decode_batch_response_wait_seconds": persistent_result.get("batch_response_wait_seconds"),
                "persistent_decode_batch_request_response_seconds": persistent_result.get("batch_request_response_seconds"),
                "persistent_decode_batch_worker_processing_seconds": persistent_result.get("batch_worker_processing_seconds"),
                "persistent_decode_batch_decode_export_duration_seconds": persistent_result.get("decode_export_duration_seconds"),
            }
            assignment_update = {
                "status": "decoded",
                "decode_start_time": decode_started_at,
                "decode_completed_time": decode_completed_at,
                "decode_elapsed_seconds": round(float(decode_elapsed), 3),
                "decoded_result_handoff_time": decode_handoff_at,
                "packets_read": stats.get("packets_read"),
                "rows_written": stats.get("rows_written"),
                "decode_worker_mode": self.decode_worker_mode,
                **persistent_fields,
            }
            chunk_update = {
                "status": "decoded_waiting_append",
                "decode_status": "decoded",
                "append_status": "waiting_for_ordered_append",
                "decoded_csv_path": item["remote_csv"],
                "local_decoded_csv_path": str(local_chunk_csv),
                "local_decoded_csv_size": None,
                "local_decoded_csv_status": "copy_deferred",
                "decoded_chunk_copy_deferred": True,
                "decoded_chunk_copy_phase": "post_live_reporting",
                "decoded_result_handoff_time": decode_handoff_at,
                "decode_started_at": decode_started_at,
                "decode_start_time": decode_started_at,
                "decode_completed_at": decode_completed_at,
                "decode_completed_time": decode_completed_at,
                "decode_complete_time": decode_completed_at,
                "decode_elapsed_seconds": round(float(decode_elapsed), 3),
                "decode_worker_mode": self.decode_worker_mode,
                "_decode_end_monotonic": decode_end,
                "local_chunk_csv_available_at": None,
                "local_copy_completed_time": None,
                "decode_export_statistics": stats,
                **persistent_fields,
            }
            with self._chunk_condition:
                assignment = self.decode_worker_assignments[int(chunk_entry.get("_decode_assignment_index") or 0)]
                assignment.update(assignment_update)
                chunk_entry.update(chunk_update)
                self._chunk_condition.notify_all()
        self.write_manifest(status="running", phase="decode_batch_complete")

    def decode_ready_chunk(self, chunk_entry: dict[str, Any], worker_id: int) -> None:
        index = int(chunk_entry["chunk_index"])
        raw_pcap = str(chunk_entry["raw_chunk_pcap_path"])
        remote_csv = f"{self.layout.remote_decoded_chunk_dir}/chunk_{index:03d}.csv"
        decode_log = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_decode.log"
        decode_started_marker = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_decode_started_at.txt"
        decode_completed_marker = f"{self.layout.remote_receiver_run_dir}/chunk_{index:03d}_decode_completed_at.txt"
        self.assert_raw_chunk_ready(index, raw_pcap)
        assigned_at = now_iso()
        with self._chunk_condition:
            assignment_index = len(self.decode_worker_assignments)
            self.decode_worker_assignments.append(
                {
                    "worker_id": worker_id,
                    "chunk_index": index,
                    "assigned_at": assigned_at,
                    "status": "decoding",
                    "decode_worker_mode": self.decode_worker_mode,
                }
            )
            chunk_entry.update(
                {
                    "status": "decoding",
                    "decode_status": "running",
                    "append_status": "waiting_for_decode",
                    "decode_worker_id": worker_id,
                    "decode_worker_mode": self.decode_worker_mode,
                    "decode_assignment_time": assigned_at,
                    "decoded_csv_path": remote_csv,
                    "decode_log_path": decode_log,
                    "decode_started_marker_path": decode_started_marker,
                    "decode_completed_marker_path": decode_completed_marker,
                    "_decode_assignment_index": assignment_index,
                }
            )
            self._chunk_condition.notify_all()
        self.write_manifest(status="running", phase="decode_drain")

        decode_start = time.monotonic()
        chunk_entry["_decode_start_monotonic"] = decode_start
        persistent_result: dict[str, Any] | None = None
        if self.persistent_decode_enabled:
            persistent_result = self.decode_with_persistent_worker(
                chunk_entry=chunk_entry,
                worker_id=worker_id,
                raw_pcap=raw_pcap,
                remote_csv=remote_csv,
                decode_log=decode_log,
                decode_started_marker=decode_started_marker,
                decode_completed_marker=decode_completed_marker,
            )
            decode_end = time.monotonic()
            stats = dict(persistent_result.get("export_stats") or {})
            decode_started_at = persistent_result.get("decode_started_at")
            decode_completed_at = persistent_result.get("decode_completed_at")
        else:
            decode_cmd = (
                f"date -u +%Y-%m-%dT%H:%M:%SZ > {shlex.quote(decode_started_marker)}; "
                + self.decode_command(index, raw_pcap, remote_csv)
                + f" > {shlex.quote(decode_log)} 2>&1; "
                + f"date -u +%Y-%m-%dT%H:%M:%SZ > {shlex.quote(decode_completed_marker)}"
            )
            self.remote_exec(self.receiver_node, decode_cmd, self.timeout, True)
            decode_end = time.monotonic()
            decode_started_at = self.remote_timestamp_or_none(self.receiver_node, decode_started_marker)
            decode_completed_at = self.remote_timestamp_or_none(self.receiver_node, decode_completed_marker)
            decode_text = self.remote_exec(self.receiver_node, f"cat {shlex.quote(decode_log)} 2>/dev/null || true", 30, False)
            stats = parse_export_stats(decode_text.stdout)
        expected_packets = chunk_entry.get("packets_expected")
        if expected_packets not in (None, "") and stats.get("packets_read") != int(expected_packets):
            raise PipelineError(f"chunk {index} decode read {stats.get('packets_read')} packets, expected {expected_packets}")

        local_chunk_csv = self.local_decoded_chunk_path(index)
        decode_handoff_at = now_iso()
        assignment_update = {
            "status": "decoded",
            "decode_start_time": decode_started_at,
            "decode_completed_time": decode_completed_at,
            "decode_elapsed_seconds": round(decode_end - decode_start, 3),
            "decoded_result_handoff_time": decode_handoff_at,
            "packets_read": stats.get("packets_read"),
            "rows_written": stats.get("rows_written"),
            "decode_worker_mode": self.decode_worker_mode,
        }
        chunk_update = {
            "status": "decoded_waiting_append",
            "decode_status": "decoded",
            "append_status": "waiting_for_ordered_append",
            "decoded_csv_path": remote_csv,
            "local_decoded_csv_path": str(local_chunk_csv),
            "local_decoded_csv_size": None,
            "local_decoded_csv_status": "copy_deferred",
            "decoded_chunk_copy_deferred": True,
            "decoded_chunk_copy_phase": "post_live_reporting",
            "decoded_result_handoff_time": decode_handoff_at,
            "decode_started_at": decode_started_at,
            "decode_start_time": decode_started_at,
            "decode_completed_at": decode_completed_at,
            "decode_completed_time": decode_completed_at,
            "decode_complete_time": decode_completed_at,
            "decode_elapsed_seconds": round(decode_end - decode_start, 3),
            "decode_worker_mode": self.decode_worker_mode,
            "_decode_end_monotonic": decode_end,
            "local_chunk_csv_available_at": None,
            "local_copy_completed_time": None,
            "decode_export_statistics": stats,
        }
        if persistent_result is not None:
            persistent_fields = {
                "persistent_decode_task_path": persistent_result.get("task_path"),
                "persistent_decode_status_path": persistent_result.get("status_path"),
                "persistent_decode_task_submit_seconds": persistent_result.get("task_submit_seconds"),
                "persistent_decode_response_wait_seconds": persistent_result.get("response_wait_seconds"),
                "persistent_decode_request_response_seconds": persistent_result.get("request_response_seconds"),
                "persistent_decode_worker_processing_seconds": persistent_result.get("worker_processing_seconds"),
                "persistent_decode_worker_id": persistent_result.get("worker_id"),
            }
            assignment_update.update(persistent_fields)
            chunk_update.update(persistent_fields)
        with self._chunk_condition:
            assignment = self.decode_worker_assignments[int(chunk_entry.get("_decode_assignment_index") or 0)]
            assignment.update(assignment_update)
            chunk_entry.update(chunk_update)
            self._chunk_condition.notify_all()
        self.write_manifest(status="running", phase="decode_drain")

    def append_decoded_chunk(self, chunk_entry: dict[str, Any], cumulative_rows_before: int) -> int:
        index = int(chunk_entry["chunk_index"])
        remote_csv = str(chunk_entry["decoded_csv_path"])
        append_started_at = now_iso()
        with self._chunk_condition:
            chunk_entry.update(
                {
                    "status": "appending",
                    "append_status": "appending",
                    "append_started_time": append_started_at,
                    "ordered_append_index": index,
                }
            )
            self._chunk_condition.notify_all()
        self.write_manifest(status="running", phase="ordered_append")

        append_start = time.monotonic()
        chunk_entry["_append_start_monotonic"] = append_start
        append_result = self.remote_exec(self.receiver_node, self.append_with_marker_command(index, remote_csv), 120, True)
        append_end = time.monotonic()
        chunk_entry["_append_end_monotonic"] = append_end
        append_completed_at = self.remote_timestamp_or_none(self.receiver_node, self.append_completed_marker_path(index)) or now_iso()
        combined_lines = parse_last_int(append_result.stdout)
        cumulative_rows = max(0, combined_lines - 1)
        rows_appended = cumulative_rows - cumulative_rows_before
        stats = chunk_entry.get("decode_export_statistics") or {}
        rows_written = int(stats.get("rows_written") or 0)
        if rows_written and rows_appended != rows_written:
            raise PipelineError(f"chunk {index} appended {rows_appended} rows, expected decoded rows {rows_written}")

        capture_end = float_or_none(chunk_entry.get("_capture_end_monotonic"))
        capture_to_output_latency = round(append_end - capture_end, 3) if capture_end is not None else None
        with self._chunk_condition:
            chunk_entry.update(
                {
                    "status": "appended",
                    "append_status": "appended",
                    "append_completed_time": append_completed_at,
                    "append_complete_time": append_completed_at,
                    "append_completed_marker_path": self.append_completed_marker_path(index),
                    "append_elapsed_seconds": round(append_end - append_start, 3),
                    "write_append_elapsed_seconds": round(append_end - append_start, 3),
                    "capture_to_output_latency_seconds": capture_to_output_latency,
                    "combined_lines_after_append": combined_lines,
                    "cumulative_rows_before": cumulative_rows_before,
                    "cumulative_rows": cumulative_rows,
                    "rows_appended": rows_appended,
                }
            )
            if not hasattr(self, "ordered_output_records"):
                self.ordered_output_records = []
            self.ordered_output_records.append(
                {
                    "chunk_index": index,
                    "ordered_output_position": len(self.ordered_output_records),
                    "status": "appended",
                    "append_completed_time": append_completed_at,
                    "rows_appended": rows_appended,
                    "cumulative_rows": cumulative_rows,
                    "write_strategy": self.output_write_strategy if hasattr(self, "config") else "ordered_output",
                }
            )
            self._chunk_condition.notify_all()
        self.write_manifest(status="running", phase="ordered_append")
        return cumulative_rows

    def mark_decoded_chunk_ready_for_ordered_merge(self, chunk_entry: dict[str, Any]) -> None:
        index = int(chunk_entry["chunk_index"])
        ready_at = now_iso()
        with self._chunk_condition:
            chunk_entry.update(
                {
                    "status": "decoded_waiting_ordered_merge",
                    "append_status": "waiting_for_ordered_merge",
                    "ordered_append_index": index,
                    "ordered_merge_ready_time": ready_at,
                }
            )
            self._chunk_condition.notify_all()
        self.write_manifest(status="running", phase="ordered_merge_buffer")

    def merge_decoded_chunks_command(self, ordered_chunks: list[dict[str, Any]]) -> str:
        inputs = [
            {"chunk_index": int(chunk["chunk_index"]), "path": str(chunk["decoded_csv_path"])}
            for chunk in ordered_chunks
        ]
        payload = {"inputs": inputs, "output": self.layout.remote_combined_csv}
        script = r'''
import json
import pathlib
import sys
import time

payload = json.loads(sys.argv[1])
inputs = payload["inputs"]
output = pathlib.Path(payload["output"])
if not inputs:
    raise SystemExit("no chunk CSV inputs provided")
output.parent.mkdir(parents=True, exist_ok=True)
partial = output.with_suffix(output.suffix + ".partial")
started = time.monotonic()
files = []
with partial.open("wb") as dst:
    for position, item in enumerate(inputs):
        chunk_index = int(item["chunk_index"])
        source = pathlib.Path(item["path"])
        if not source.is_file() or source.stat().st_size <= 0:
            raise SystemExit(f"chunk {chunk_index} CSV is missing or empty: {source}")
        file_started = time.monotonic()
        source_lines = 0
        lines_written = 0
        bytes_written = 0
        with source.open("rb") as src:
            if position > 0:
                header = src.readline()
                if not header:
                    raise SystemExit(f"chunk {chunk_index} CSV has no header: {source}")
                source_lines += 1
            while True:
                block = src.read(1024 * 1024)
                if not block:
                    break
                source_lines += block.count(b"\n")
                lines_written += block.count(b"\n")
                bytes_written += len(block)
                dst.write(block)
        files.append(
            {
                "chunk_index": chunk_index,
                "path": str(source),
                "source_lines_observed": source_lines,
                "lines_written": lines_written,
                "rows_appended": max(0, lines_written - 1) if position == 0 else lines_written,
                "bytes_written": bytes_written,
                "write_elapsed_seconds": round(time.monotonic() - file_started, 6),
            }
        )
partial.replace(output)
with output.open("rb") as handle:
    combined_lines = sum(1 for _ in handle)
elapsed = round(time.monotonic() - started, 6)
print(
    json.dumps(
        {
            "status": "complete",
            "output": str(output),
            "input_count": len(inputs),
            "combined_lines": combined_lines,
            "combined_rows": max(0, combined_lines - 1),
            "merge_elapsed_seconds": elapsed,
            "files": files,
        },
        sort_keys=True,
    )
)
'''
        return shell_join(["python3", "-c", script, json.dumps(payload, separators=(",", ":"))])

    def merge_decoded_chunks_ordered(self, ordered_chunks: list[dict[str, Any]]) -> int:
        if not ordered_chunks:
            return 0
        ordered = sorted(ordered_chunks, key=lambda item: int(item.get("chunk_index", -1)))
        indices = [int(chunk["chunk_index"]) for chunk in ordered]
        expected = list(range(indices[0], indices[0] + len(indices)))
        if indices != expected:
            raise PipelineError(f"ordered merge received non-contiguous chunk indexes: {indices}")

        merge_started_at = now_iso()
        merge_start = time.monotonic()
        with self._chunk_condition:
            for chunk_entry in ordered:
                chunk_entry.update(
                    {
                        "status": "merging_ordered_output",
                        "append_status": "merging_ordered_output",
                        "append_started_time": merge_started_at,
                        "ordered_merge_started_time": merge_started_at,
                    }
                )
            self._chunk_condition.notify_all()
        self.write_manifest(status="running", phase="ordered_merge")

        result = self.remote_exec(self.receiver_node, self.merge_decoded_chunks_command(ordered), 300, False)
        merge_end = time.monotonic()
        merge_completed_at = now_iso()
        if result.returncode != 0:
            raise PipelineError(
                "ordered chunk merge failed: "
                + f"stdout:\n{result.stdout[-2000:]}\nstderr:\n{result.stderr[-2000:]}"
            )
        try:
            merge_status = extract_json_object(result.stdout)
        except (ValueError, json.JSONDecodeError) as error:
            raise PipelineError(f"ordered chunk merge did not return JSON: {result.stdout[-2000:]}") from error
        if merge_status.get("status") != "complete":
            raise PipelineError(f"ordered chunk merge returned non-complete status: {merge_status}")

        files_by_index = {int(item["chunk_index"]): item for item in merge_status.get("files") or []}
        cumulative_rows = 0
        merge_elapsed = float_or_none(merge_status.get("merge_elapsed_seconds"))
        if merge_elapsed is None:
            merge_elapsed = round(merge_end - merge_start, 6)
        for chunk_entry in ordered:
            index = int(chunk_entry["chunk_index"])
            file_metrics = files_by_index.get(index)
            if file_metrics is None:
                raise PipelineError(f"ordered merge returned no file metrics for chunk {index}")
            rows_appended = int(file_metrics.get("rows_appended") or 0)
            stats = chunk_entry.get("decode_export_statistics") or {}
            rows_written = int(stats.get("rows_written") or 0)
            if rows_written and rows_appended != rows_written:
                raise PipelineError(f"chunk {index} merged {rows_appended} rows, expected decoded rows {rows_written}")
            cumulative_rows_before = cumulative_rows
            cumulative_rows += rows_appended
            capture_end = float_or_none(chunk_entry.get("_capture_end_monotonic"))
            capture_to_output_latency = round(merge_end - capture_end, 3) if capture_end is not None else None
            chunk_write_elapsed = round(float(file_metrics.get("write_elapsed_seconds") or 0.0), 6)
            with self._chunk_condition:
                chunk_entry.update(
                    {
                        "status": "appended",
                        "append_status": "appended",
                        "append_completed_time": merge_completed_at,
                        "append_complete_time": merge_completed_at,
                        "append_completed_marker_path": None,
                        "append_elapsed_seconds": chunk_write_elapsed,
                        "write_append_elapsed_seconds": chunk_write_elapsed,
                        "ordered_merge_elapsed_seconds": round(float(merge_elapsed), 6),
                        "ordered_merge_request_response_seconds": round(merge_end - merge_start, 6),
                        "capture_to_output_latency_seconds": capture_to_output_latency,
                        "combined_lines_after_append": cumulative_rows + 1,
                        "cumulative_rows_before": cumulative_rows_before,
                        "cumulative_rows": cumulative_rows,
                        "rows_appended": rows_appended,
                        "ordered_merge_file_metrics": file_metrics,
                    }
                )
                if not hasattr(self, "ordered_output_records"):
                    self.ordered_output_records = []
                self.ordered_output_records.append(
                    {
                        "chunk_index": index,
                        "ordered_output_position": len(self.ordered_output_records),
                        "status": "appended",
                        "append_completed_time": merge_completed_at,
                        "rows_appended": rows_appended,
                        "cumulative_rows": cumulative_rows,
                        "write_strategy": self.output_write_strategy if hasattr(self, "config") else "ordered_output",
                    }
                )
                self._chunk_condition.notify_all()

        combined_rows = int(merge_status.get("combined_rows") or 0)
        if combined_rows != cumulative_rows:
            raise PipelineError(f"ordered merge produced {combined_rows} rows, expected {cumulative_rows}")
        merge_record = {
            "strategy": "ordered_chunk_merge",
            "status": "complete",
            "chunk_count": len(ordered),
            "chunk_indices": indices,
            "started_at": merge_started_at,
            "completed_at": merge_completed_at,
            "merge_elapsed_seconds": round(float(merge_elapsed), 6),
            "merge_request_response_seconds": round(merge_end - merge_start, 6),
            "combined_lines": merge_status.get("combined_lines"),
            "combined_rows": combined_rows,
            "output_csv": self.layout.remote_combined_csv,
            "files": merge_status.get("files") or [],
        }
        with self._state_lock:
            self.output_merge_records.append(merge_record)
        self.write_manifest(status="running", phase="ordered_merge_complete")
        return cumulative_rows

    def decode_append_copy_ready_chunk(self, chunk_entry: dict[str, Any], cumulative_rows_before: int) -> int:
        self.decode_ready_chunk(chunk_entry, worker_id=1)
        return self.append_decoded_chunk(chunk_entry, cumulative_rows_before)

    def collect_persistent_decode_batch(
        self,
        work_queue: queue.Queue[dict[str, Any] | None],
        first_item: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], int, bool, dict[str, Any]]:
        """Collect one bounded persistent-worker batch from completed chunks."""
        collection_started_at = now_iso()
        collection_started = time.monotonic()
        items: list[dict[str, Any]] = [first_item]
        task_done_count = 1
        stop_after_batch = False
        batch_limit = self.persistent_decode_batch_chunks
        collect_timeout = self.persistent_decode_batch_collect_timeout_seconds
        deadline = collection_started + collect_timeout
        collection = {
            "configured_batch_limit": batch_limit,
            "requested_batch_size_chunks": batch_limit,
            "collect_timeout_seconds": collect_timeout,
            "batch_formation_started_at": collection_started_at,
            "queue_depth_after_first_get": work_queue.qsize(),
            "queue_ready_drained_count": 0,
            "queue_depth_after_drain": work_queue.qsize(),
        }
        self.note_queue_get("raw_to_decode", work_queue)
        flush_reason = "requested_batch_size_reached" if batch_limit <= 1 else "bounded_wait_expired"

        while len(items) < batch_limit:
            try:
                if collect_timeout <= 0.0:
                    extra = work_queue.get_nowait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0.0:
                        flush_reason = "bounded_wait_expired"
                        break
                    extra = work_queue.get(timeout=remaining)
            except queue.Empty:
                flush_reason = "no_ready_chunk" if collect_timeout <= 0.0 else "bounded_wait_expired"
                break
            task_done_count += 1
            if extra is None:
                stop_after_batch = True
                flush_reason = "end_of_input"
                break
            self.note_queue_get("raw_to_decode", work_queue)
            items.append(extra)
            collection["queue_ready_drained_count"] = int(collection["queue_ready_drained_count"]) + 1
            collection["queue_depth_after_drain"] = work_queue.qsize()
            if len(items) >= batch_limit:
                flush_reason = "requested_batch_size_reached"
                break

        collection_ended = time.monotonic()
        collection_ended_at = now_iso()
        chunk_ids = [int(item.get("chunk_index", -1)) for item in items]
        actual_batch_size = len(items)
        collection.update(
            {
                "actual_batch_size": actual_batch_size,
                "actual_batch_size_chunks": actual_batch_size,
                "multi_chunk_batch": actual_batch_size > 1,
                "singleton_batch": actual_batch_size == 1,
                "partial_batch": actual_batch_size < batch_limit,
                "flush_reason": flush_reason,
                "end_of_input_seen": stop_after_batch,
                "chunk_indices": chunk_ids,
                "chunk_ids": chunk_ids,
                "queue_depth_after_drain": work_queue.qsize(),
                "batch_formation_completed_at": collection_ended_at,
                "batch_formation_timestamp": collection_ended_at,
                "batch_wait_time_seconds": round(collection_ended - collection_started, 6),
                "batch_collect_wait_seconds": round(collection_ended - collection_started, 6),
            }
        )
        return items, task_done_count, stop_after_batch, collection

    def concurrent_decode_worker(
        self,
        worker_id: int,
        work_queue: queue.Queue[dict[str, Any] | None],
        decoded_queue: queue.Queue[dict[str, Any] | None],
        errors: list[BaseException],
    ) -> None:
        while True:
            items: list[dict[str, Any]] = []
            task_done_count = 0
            stop_after_batch = False
            try:
                item = work_queue.get()
                if item is None:
                    work_queue.task_done()
                    return

                if self.persistent_decode_enabled:
                    items, task_done_count, stop_after_batch, batch_collection = self.collect_persistent_decode_batch(
                        work_queue,
                        item,
                    )
                    self.decode_ready_chunks(items, worker_id=worker_id, batch_collection=batch_collection)
                else:
                    task_done_count = 1
                    self.note_queue_get("raw_to_decode", work_queue)
                    items.append(item)
                    for ready_item in items:
                        self.decode_ready_chunk(ready_item, worker_id=worker_id)

                for ready_item in items:
                    self.put_with_backpressure(
                        decoded_queue,
                        ready_item,
                        "decoded_to_writer",
                        chunk_index=int(ready_item.get("chunk_index", -1)),
                    )
                if stop_after_batch:
                    return
            except BaseException as error:
                with self._chunk_condition:
                    errors.append(error)
                    self._chunk_condition.notify_all()
                return
            finally:
                for _ in range(task_done_count):
                    work_queue.task_done()

    def concurrent_ordered_append_coordinator(
        self,
        producer_done: threading.Event,
        decoded_queue: queue.Queue[dict[str, Any] | None],
        errors: list[BaseException],
    ) -> None:
        cumulative_rows = 0
        next_index = 0
        pending_decoded: dict[int, dict[str, Any]] = {}
        ordered_merge_ready: list[dict[str, Any]] = []
        merge_done = False
        use_ordered_merge = self.output_write_strategy == "ordered_chunk_merge"
        while True:
            try:
                while next_index in pending_decoded:
                    next_chunk = pending_decoded.pop(next_index)
                    self.max_ordered_write_buffer_depth_chunks = max(
                        self.max_ordered_write_buffer_depth_chunks,
                        len(pending_decoded),
                    )
                    if use_ordered_merge:
                        self.mark_decoded_chunk_ready_for_ordered_merge(next_chunk)
                        ordered_merge_ready.append(next_chunk)
                    else:
                        cumulative_rows = self.append_decoded_chunk(next_chunk, cumulative_rows)
                    next_index += 1
                if errors:
                    return
                if producer_done.is_set() and next_index >= len(self.chunks):
                    if use_ordered_merge and not merge_done:
                        cumulative_rows = self.merge_decoded_chunks_ordered(ordered_merge_ready)
                        merge_done = True
                    return
                try:
                    item = decoded_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                try:
                    if item is None:
                        continue
                    self.note_queue_get("decoded_to_writer", decoded_queue)
                    chunk_index = int(item.get("chunk_index", -1))
                    if chunk_index in pending_decoded or any(int(chunk.get("chunk_index", -1)) == chunk_index for chunk in ordered_merge_ready):
                        raise PipelineError(f"duplicate decoded chunk delivered to ordered writer: {chunk_index}")
                    pending_decoded[chunk_index] = item
                    self.max_ordered_write_buffer_depth_chunks = max(
                        self.max_ordered_write_buffer_depth_chunks,
                        len(pending_decoded),
                    )
                finally:
                    decoded_queue.task_done()
            except BaseException as error:
                with self._chunk_condition:
                    errors.append(error)
                    self._chunk_condition.notify_all()
                return

    @staticmethod
    def interval_overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> float:
        return max(0.0, min(a_end, b_end) - max(a_start, b_start))

    def add_overlap_metrics(self) -> dict[str, Any]:
        with self._state_lock:
            chunks = [chunk for chunk in self.chunks if chunk.get("decode_status") == "decoded"]
            if not chunks:
                metrics = {
                    "capture_decode_overlap_seconds": 0.0,
                    "capture_decode_overlap_observed": False,
                    "reason": "no decoded chunks",
                }
                self.manifest["overlap_metrics"] = metrics
                return metrics
            required = ["_capture_start_monotonic", "_capture_end_monotonic", "_decode_start_monotonic", "_decode_end_monotonic"]
            if any(any(key not in chunk for key in required) for chunk in chunks):
                metrics = {
                    "capture_decode_overlap_seconds": 0.0,
                    "capture_decode_overlap_observed": False,
                    "reason": "timestamp evidence incomplete",
                }
                self.manifest["overlap_metrics"] = metrics
                return metrics
            capture_intervals = [
                (float(chunk["_capture_start_monotonic"]), float(chunk["_capture_end_monotonic"]))
                for chunk in chunks
            ]
            total_overlap = 0.0
            max_decode_lag = 0.0
            max_decode_lag_chunks = 0
            for chunk in chunks:
                decode_start = float(chunk["_decode_start_monotonic"])
                decode_end = float(chunk["_decode_end_monotonic"])
                overlap = sum(self.interval_overlap(decode_start, decode_end, start, end) for start, end in capture_intervals)
                chunk["decode_capture_overlap_seconds"] = round(overlap, 3)
                total_overlap += overlap
                max_decode_lag = max(max_decode_lag, max(0.0, decode_end - float(chunk["_capture_end_monotonic"])))
                captured_by_decode_end = sum(1 for item in chunks if float(item["_capture_end_monotonic"]) <= decode_end)
                decoded_count = len([item for item in chunks if float(item["_decode_end_monotonic"]) <= decode_end])
                max_decode_lag_chunks = max(max_decode_lag_chunks, max(0, captured_by_decode_end - decoded_count))
            first_capture_start = min(float(chunk["_capture_start_monotonic"]) for chunk in chunks)
            last_capture_end = max(float(chunk["_capture_end_monotonic"]) for chunk in chunks)
            first_decode_start = min(float(chunk["_decode_start_monotonic"]) for chunk in chunks)
            last_decode_end = max(float(chunk["_decode_end_monotonic"]) for chunk in chunks)
            metrics = {
                "capture_decode_overlap_seconds": round(total_overlap, 3),
                "capture_decode_overlap_observed": total_overlap > 0,
                "elapsed_capture_wall_seconds": round(last_capture_end - first_capture_start, 3),
                "elapsed_decode_wall_seconds": round(last_decode_end - first_decode_start, 3),
                "decode_finished_after_capture_seconds": round(max(0.0, last_decode_end - last_capture_end), 3),
                "maximum_decode_lag_seconds": round(max_decode_lag, 3),
                "maximum_decode_lag_chunks": max_decode_lag_chunks,
            }
            self.manifest["overlap_metrics"] = metrics
            return metrics

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

    def chunk_float_values(self, key: str) -> list[float]:
        values: list[float] = []
        for chunk in self.chunks:
            value = float_or_none(chunk.get(key))
            if value is not None:
                values.append(value)
        return values

    def total_sender_packets(self, key: str) -> int:
        return sum(int_or_zero((chunk.get("sender_statistics") or {}).get(key)) for chunk in self.chunks)

    def total_export_packets(self, key: str) -> int:
        return sum(int_or_zero((chunk.get("decode_export_statistics") or {}).get(key)) for chunk in self.chunks)

    @property
    def require_first_class_metrics(self) -> bool:
        reporting = self.config.get("reporting") or {}
        return bool(reporting.get("require_first_class_metrics"))

    def metric_output_paths(self) -> dict[str, Any]:
        return {
            "manifest_json": str(self.layout.manifest_json),
            "summary_json": str(self.layout.summary_json),
            "measurement_csv": str(self.layout.measurement_csv),
            "queue_history_csv": str(self.layout.queue_history_csv),
            "validation_json": str(self.layout.validation_json),
            "report_mirror_dir": str(self.layout.report_mirror_dir) if self.layout.report_mirror_dir else None,
            "combined_csv": str(self.layout.local_combined_csv),
            "local_decoded_chunk_dir": str(self.layout.local_decoded_chunk_dir),
            "visualization_dir": str(self.layout.viz_dir),
            "remote_combined_csv": self.layout.remote_combined_csv,
            "remote_raw_chunk_dir": self.layout.remote_raw_chunk_dir,
            "remote_decoded_chunk_dir": self.layout.remote_decoded_chunk_dir,
            "remote_decode_task_dir": self.layout.remote_decode_task_dir,
            "remote_decode_status_dir": self.layout.remote_decode_status_dir,
            "remote_validation_json": f"{self.layout.remote_receiver_run_dir}/validation.json",
        }

    def write_remote_json(self, node: str, remote_path: str, payload: dict[str, Any], timeout: int = 60) -> None:
        encoded = base64.b64encode((json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")).decode("ascii")
        script = (
            "import base64, pathlib, sys; "
            "path = pathlib.Path(sys.argv[1]); "
            "path.parent.mkdir(parents=True, exist_ok=True); "
            "path.write_bytes(base64.b64decode(sys.argv[2]))"
        )
        command = f"python3 -c {shlex.quote(script)} {shlex.quote(remote_path)} {shlex.quote(encoded)}"
        self.remote_exec(node, command, timeout, True)

    def mirror_current_reports(self) -> list[str]:
        return mirror_report_files(self.layout.report_dir, self.layout.report_mirror_dir, RUN_REPORT_FILES)

    def local_decoded_chunk_path(self, index: int) -> Path:
        return self.layout.local_decoded_chunk_dir / f"chunk_{index:03d}.csv"

    def mirror_decoded_chunks_for_reporting(self) -> dict[str, Any]:
        with self._state_lock:
            decoded_chunks = sorted(
                [
                    chunk
                    for chunk in self.chunks
                    if chunk.get("decode_status") == "decoded" and chunk.get("decoded_csv_path")
                ],
                key=lambda item: int(item.get("chunk_index", -1)),
            )
        started_at = now_iso()
        started = time.monotonic()
        result: dict[str, Any] = {
            "status": "running",
            "phase": "post_live_reporting",
            "critical_path": False,
            "started_at": started_at,
            "local_decoded_chunk_dir": str(self.layout.local_decoded_chunk_dir),
            "remote_decoded_chunk_dir": self.layout.remote_decoded_chunk_dir,
            "chunk_count": len(decoded_chunks),
            "chunks_copied": 0,
            "files": [],
        }
        self.manifest["decoded_chunk_mirroring"] = result
        self.write_manifest(status="running", phase="decoded_chunk_reporting")
        for chunk_entry in decoded_chunks:
            index = int(chunk_entry["chunk_index"])
            remote_csv = str(chunk_entry["decoded_csv_path"])
            local_chunk_csv = self.local_decoded_chunk_path(index)
            copy_started_at = now_iso()
            with self._chunk_condition:
                chunk_entry.update(
                    {
                        "local_decoded_csv_path": str(local_chunk_csv),
                        "local_decoded_csv_status": "copying_for_reporting",
                        "decoded_chunk_copy_deferred": True,
                        "decoded_chunk_copy_phase": "post_live_reporting",
                        "local_copy_started_time": copy_started_at,
                    }
                )
                self._chunk_condition.notify_all()
            self.remote_copy_from(self.receiver_node, remote_csv, local_chunk_csv, 300)
            copy_completed_at = now_iso()
            file_info = {
                "chunk_index": index,
                "remote_csv": remote_csv,
                "local_csv": str(local_chunk_csv),
                "bytes": local_chunk_csv.stat().st_size if local_chunk_csv.exists() else None,
                "copy_started_time": copy_started_at,
                "copy_completed_time": copy_completed_at,
            }
            result["files"].append(file_info)
            result["chunks_copied"] = len(result["files"])
            with self._chunk_condition:
                chunk_entry.update(
                    {
                        "local_decoded_csv_path": str(local_chunk_csv),
                        "local_decoded_csv_size": file_info["bytes"],
                        "local_decoded_csv_status": "available",
                        "local_chunk_csv_available_at": copy_completed_at,
                        "local_copy_completed_time": copy_completed_at,
                    }
                )
                self._chunk_condition.notify_all()
        result.update(
            {
                "status": "complete",
                "completed_at": now_iso(),
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
        )
        self.manifest["decoded_chunk_mirroring"] = result
        return result

    def write_report_bundle(self, summary: dict[str, Any]) -> None:
        if summary.get("validation") is not None:
            atomic_write_json(self.layout.validation_json, summary["validation"])
        if summary.get("measurement_summary") is not None:
            self.write_local_measurement_files(summary)
        atomic_write_json(self.layout.summary_json, summary)

    def build_dry_run_measurement_summary(self, *, validation: dict[str, Any]) -> dict[str, Any]:
        queue_metrics = self.queue_metrics()
        input_rate_pps = None
        if self.source_mode == "pcap_replay":
            input_rate_pps = int_or_zero(self.config.get("pcap_replay", {}).get("pps")) or None
        summary = {
            "schema_version": 1,
            "dry_run": True,
            "dry_run_placeholder_metrics": True,
            "event_id": self.config.get("event_id"),
            "source_mode": self.source_mode,
            "pipeline_mode": self.pipeline_mode,
            "decode_worker_mode": self.decode_worker_mode,
            "decoder_workers": self.decode_workers,
            "queue_capacity_chunks": self.queue_capacity_chunks,
            "bounded_queues": queue_metrics.get("bounded_queues"),
            "bounded_queue_names": queue_metrics.get("bounded_queue_names"),
            "output_write_strategy": self.output_write_strategy,
            "input_rate_pps": input_rate_pps,
            "packets_per_repeat": self.config.get("pcap_replay", {}).get("packet_limit") if self.source_mode == "pcap_replay" else None,
            "chunk_packets": self.chunk_packets,
            "sent_packets": 0,
            "sender_failed_packets": 0,
            "captured_packets": 0,
            "decoded_packets": 0,
            "decoded_rows": 0,
            "validation_pass": validation.get("pass"),
            "validation_returncode": validation.get("returncode"),
            "tcpdump_drops": queue_metrics.get("raw_packet_drops"),
            "max_queue_depth_chunks": queue_metrics.get("max_queue_depth_chunks"),
            "final_queue_depth_chunks": queue_metrics.get("final_queue_depth_chunks"),
            "queue_returned_to_zero": queue_metrics.get("queue_returned_to_zero"),
            "append_backlog_chunks": queue_metrics.get("append_backlog_chunks"),
            "backlog_over_time_samples": list(self.queue_metrics_history),
            "backpressure_blocked_put_count": 0,
            "backpressure_blocking_seconds": 0.0,
            "writer_queue_depth_chunks": queue_metrics.get("writer_queue_depth_chunks"),
            "writer_queue_max_depth_after_put_chunks": queue_metrics.get("writer_queue_max_depth_after_put_chunks"),
            "writer_queue_blocked_put_count": 0,
            "writer_queue_blocking_seconds": 0.0,
            "backpressure": queue_metrics.get("backpressure") or {},
            "capture_time_seconds": dry_metric_stats(),
            "decode_time_seconds": dry_metric_stats(),
            "persistent_decode_worker_startup_seconds": dry_metric_stats(),
            "persistent_decode_request_response_seconds": dry_metric_stats(),
            "persistent_decode_worker_processing_seconds": dry_metric_stats(),
            "requested_batch_size_chunks": self.persistent_decode_batch_chunks if self.persistent_decode_enabled else None,
            "persistent_decode_batch_chunks_configured": self.persistent_decode_batch_chunks if self.persistent_decode_enabled else None,
            "persistent_decode_batch_collect_timeout_seconds": self.persistent_decode_batch_collect_timeout_seconds if self.persistent_decode_enabled else None,
            "decoder_request_count": 0,
            "batch_count": 0,
            "persistent_decode_batch_count": 0,
            "persistent_decode_multi_chunk_batch_count": 0,
            "singleton_batch_count": 0,
            "partial_batch_count": 0,
            "persistent_decode_singleton_batch_count": 0,
            "persistent_decode_partial_batch_count": 0,
            "persistent_decode_batch_size": dry_metric_stats(),
            "actual_batch_size_chunks": dry_metric_stats(),
            "persistent_decode_actual_batch_size": dry_metric_stats(),
            "persistent_decode_batch_request_response_seconds": dry_metric_stats(),
            "persistent_decode_batch_worker_processing_seconds": dry_metric_stats(),
            "persistent_decode_batch_records": [],
            "persistent_decode_batching_accounting": {
                "expected_chunk_ids": [],
                "every_chunk_submitted_exactly_once": True,
                "every_result_returned_exactly_once": True,
                "ordered_output_includes_every_chunk_exactly_once": True,
                "dry_run_placeholder": True,
            },
            "every_chunk_submitted_exactly_once": True,
            "every_result_returned_exactly_once": True,
            "ordered_output_includes_every_chunk_exactly_once": True,
            "persistent_decode_worker_failures": 0,
            "persistent_decode_fallback_used": False,
            "write_append_time_seconds": dry_metric_stats(),
            "ordered_merge_time_seconds": dry_metric_stats(),
            "ordered_merge_request_response_seconds": dry_metric_stats(),
            "ordered_merge_count": 0,
            "validation_time_seconds": 0.0,
            "visualization_update_time_seconds": 0.0,
            "capture_to_output_latency_seconds": dry_metric_stats(),
            "total_runtime_seconds": 0.0,
            "validated_output_throughput": {
                "validation_pass_required": True,
                "validated_packets": 0,
                "validated_rows": 0,
                "measurement_window_seconds": 0.0,
                "packets_per_second": 0.0,
                "rows_per_second": 0.0,
                "input_rate_pps": input_rate_pps,
                "keeps_up_with_input_rate": False,
                "dry_run_placeholder": True,
            },
            "output_paths": self.metric_output_paths(),
            "required_metric_gaps": [],
            "missing_metrics": [],
            "visualization": {"enabled": bool(self.config.get("visualization", {}).get("enabled", True)), "dry_run_placeholder": True},
        }
        summary["required_metric_fields_emitted"] = not self.required_metric_gaps(summary, allow_placeholders=True)
        return summary

    def required_metric_gaps(self, measurement: dict[str, Any], *, allow_placeholders: bool = False) -> list[str]:
        gaps: list[str] = []
        for key in (
            "backpressure_blocked_put_count",
            "backpressure_blocking_seconds",
            "validation_time_seconds",
            "total_runtime_seconds",
        ):
            if measurement.get(key) in (None, ""):
                gaps.append(key)

        for key in (
            "capture_time_seconds",
            "decode_time_seconds",
            "write_append_time_seconds",
            "capture_to_output_latency_seconds",
        ):
            stats = measurement.get(key)
            if not isinstance(stats, dict):
                gaps.append(key)
                continue
            if allow_placeholders and stats.get("dry_run_placeholder"):
                continue
            if int_or_zero(stats.get("count")) <= 0:
                gaps.append(f"{key}.count")
            for subkey in ("total", "max"):
                if stats.get(subkey) in (None, ""):
                    gaps.append(f"{key}.{subkey}")

        throughput = measurement.get("validated_output_throughput")
        if not isinstance(throughput, dict):
            gaps.append("validated_output_throughput")
        else:
            for key in (
                "validation_pass_required",
                "validated_packets",
                "validated_rows",
                "measurement_window_seconds",
                "packets_per_second",
                "rows_per_second",
                "keeps_up_with_input_rate",
            ):
                if throughput.get(key) in (None, ""):
                    gaps.append(f"validated_output_throughput.{key}")
            if self.source_mode == "pcap_replay" and throughput.get("input_rate_pps") in (None, ""):
                gaps.append("validated_output_throughput.input_rate_pps")

        if self.persistent_decode_enabled:
            for key in (
                "requested_batch_size_chunks",
                "decoder_request_count",
                "batch_count",
                "singleton_batch_count",
                "partial_batch_count",
                "every_chunk_submitted_exactly_once",
                "every_result_returned_exactly_once",
                "ordered_output_includes_every_chunk_exactly_once",
            ):
                if measurement.get(key) in (None, ""):
                    gaps.append(key)
            actual_batch_size = measurement.get("actual_batch_size_chunks")
            if not isinstance(actual_batch_size, dict):
                gaps.append("actual_batch_size_chunks")
            elif not (allow_placeholders and actual_batch_size.get("dry_run_placeholder")):
                if int_or_zero(actual_batch_size.get("count")) <= 0:
                    gaps.append("actual_batch_size_chunks.count")
                for subkey in ("min", "mean", "p50", "p95", "max"):
                    if actual_batch_size.get(subkey) in (None, ""):
                        gaps.append(f"actual_batch_size_chunks.{subkey}")

        return gaps

    def assert_required_metrics_present(self, measurement: dict[str, Any]) -> None:
        if not self.require_first_class_metrics:
            return
        gaps = list(measurement.get("required_metric_gaps") or self.required_metric_gaps(measurement))
        if gaps:
            raise PipelineError("required first-class metrics missing/null: " + ", ".join(gaps))

    def build_measurement_summary(
        self,
        *,
        validation: dict[str, Any],
        visualization: dict[str, Any] | None,
        total_runtime_seconds: float,
        visualization_elapsed_seconds: float | None,
    ) -> dict[str, Any]:
        queue_metrics = self.queue_metrics()
        captured_packets = sum(int_or_zero(chunk.get("packets_captured")) for chunk in self.chunks)
        decoded_rows = sum(int_or_zero((chunk.get("decode_export_statistics") or {}).get("rows_written")) for chunk in self.chunks)
        validation_pass = bool(validation.get("pass")) if validation.get("enabled") else None
        validated_packets = self.total_export_packets("packets_read") if validation_pass else 0
        validated_rows = int_or_zero(validation.get("generated_rows")) if validation_pass else 0
        input_rate_pps = None
        if self.source_mode == "pcap_replay":
            input_rate_pps = int_or_zero(self.config.get("pcap_replay", {}).get("pps")) or None
        throughput_packets = round(validated_packets / total_runtime_seconds, 3) if total_runtime_seconds > 0 else 0.0
        throughput_rows = round(validated_rows / total_runtime_seconds, 3) if total_runtime_seconds > 0 else 0.0
        backpressure_channels = queue_metrics.get("backpressure") or {}
        backpressure_blocked_put_count = sum(
            int_or_zero(channel.get("blocked_put_count")) for channel in backpressure_channels.values()
        )
        backpressure_blocking_seconds = round(
            sum(float(channel.get("total_blocked_put_seconds") or 0.0) for channel in backpressure_channels.values()),
            6,
        )
        capture_times = self.chunk_float_values("capture_elapsed_seconds")
        decode_times = self.chunk_float_values("decode_elapsed_seconds")
        persistent_request_times = self.chunk_float_values("persistent_decode_request_response_seconds")
        persistent_processing_times = self.chunk_float_values("persistent_decode_worker_processing_seconds")
        persistent_batch_sizes = [
            float(batch.get("batch_size"))
            for batch in self.persistent_decode_batches
            if float_or_none(batch.get("batch_size")) is not None
        ]
        persistent_actual_batch_sizes = [
            float(batch.get("actual_batch_size", batch.get("batch_size")))
            for batch in self.persistent_decode_batches
            if float_or_none(batch.get("actual_batch_size", batch.get("batch_size"))) is not None
        ]
        persistent_multi_chunk_batch_count = sum(
            1 for batch in self.persistent_decode_batches if int_or_zero(batch.get("actual_batch_size", batch.get("batch_size"))) > 1
        )
        persistent_decoder_request_count = len(self.persistent_decode_batches)
        persistent_singleton_batch_count = sum(
            1 for batch in self.persistent_decode_batches if int_or_zero(batch.get("actual_batch_size_chunks", batch.get("actual_batch_size", batch.get("batch_size")))) == 1
        )
        persistent_partial_batch_count = sum(
            1
            for batch in self.persistent_decode_batches
            if int_or_zero(batch.get("actual_batch_size_chunks", batch.get("actual_batch_size", batch.get("batch_size"))))
            < int_or_zero(batch.get("requested_batch_size_chunks", batch.get("configured_batch_limit", self.persistent_decode_batch_chunks)))
        )
        persistent_batching_accounting = self.chunk_delivery_accounting()
        persistent_batch_records = copy.deepcopy(self.persistent_decode_batches)
        persistent_batch_request_times = [
            float(batch.get("batch_request_response_seconds"))
            for batch in self.persistent_decode_batches
            if float_or_none(batch.get("batch_request_response_seconds")) is not None
        ]
        persistent_batch_processing_times = [
            float(batch.get("batch_worker_processing_seconds"))
            for batch in self.persistent_decode_batches
            if float_or_none(batch.get("batch_worker_processing_seconds")) is not None
        ]
        persistent_startup_times = [
            float(worker.get("startup_elapsed_seconds"))
            for worker in self.persistent_decode_workers
            if float_or_none(worker.get("startup_elapsed_seconds")) is not None
        ]
        append_times = self.chunk_float_values("append_elapsed_seconds")
        merge_times = [
            float(record.get("merge_elapsed_seconds"))
            for record in self.output_merge_records
            if float_or_none(record.get("merge_elapsed_seconds")) is not None
        ]
        merge_request_times = [
            float(record.get("merge_request_response_seconds"))
            for record in self.output_merge_records
            if float_or_none(record.get("merge_request_response_seconds")) is not None
        ]
        writer_backpressure = backpressure_channels.get("decoded_to_writer", {})
        latencies = self.chunk_float_values("capture_to_output_latency_seconds")
        validation_elapsed = float_or_none(validation.get("validation_elapsed_seconds"))
        missing: list[str] = []
        if not capture_times:
            missing.append("capture time")
        if not decode_times:
            missing.append("decode time")
        if not append_times:
            missing.append("write/append time")
        if validation.get("enabled") and validation_elapsed is None:
            missing.append("validation time")
        if not latencies:
            missing.append("capture-to-output latency")
        summary = {
            "schema_version": 1,
            "event_id": self.config.get("event_id"),
            "source_mode": self.source_mode,
            "pipeline_mode": self.pipeline_mode,
            "decode_worker_mode": self.decode_worker_mode,
            "decoder_workers": self.decode_workers,
            "queue_capacity_chunks": self.queue_capacity_chunks,
            "bounded_queues": queue_metrics.get("bounded_queues"),
            "bounded_queue_names": queue_metrics.get("bounded_queue_names"),
            "output_write_strategy": self.output_write_strategy,
            "input_rate_pps": input_rate_pps,
            "packets_per_repeat": self.config.get("pcap_replay", {}).get("packet_limit") if self.source_mode == "pcap_replay" else None,
            "chunk_packets": self.chunk_packets,
            "sent_packets": self.total_sender_packets("successful_packets"),
            "sender_failed_packets": self.total_sender_packets("failed_packets"),
            "captured_packets": captured_packets,
            "decoded_packets": self.total_export_packets("packets_read"),
            "decoded_rows": decoded_rows,
            "validation_pass": validation_pass,
            "validation_returncode": validation.get("returncode"),
            "tcpdump_drops": queue_metrics.get("raw_packet_drops"),
            "max_queue_depth_chunks": queue_metrics.get("max_queue_depth_chunks"),
            "final_queue_depth_chunks": queue_metrics.get("final_queue_depth_chunks"),
            "queue_returned_to_zero": queue_metrics.get("queue_returned_to_zero"),
            "append_backlog_chunks": queue_metrics.get("append_backlog_chunks"),
            "backlog_over_time_samples": list(self.queue_metrics_history),
            "backpressure_blocked_put_count": backpressure_blocked_put_count,
            "backpressure_blocking_seconds": backpressure_blocking_seconds,
            "writer_queue_depth_chunks": writer_backpressure.get("current_depth"),
            "writer_queue_max_depth_after_put_chunks": writer_backpressure.get("max_depth_after_put"),
            "writer_queue_blocked_put_count": int_or_zero(writer_backpressure.get("blocked_put_count")),
            "writer_queue_blocking_seconds": round(float(writer_backpressure.get("total_blocked_put_seconds") or 0.0), 6),
            "backpressure": backpressure_channels,
            "capture_time_seconds": metric_stats(capture_times),
            "decode_time_seconds": metric_stats(decode_times),
            "persistent_decode_worker_startup_seconds": metric_stats(persistent_startup_times),
            "persistent_decode_request_response_seconds": metric_stats(persistent_request_times),
            "persistent_decode_worker_processing_seconds": metric_stats(persistent_processing_times),
            "requested_batch_size_chunks": self.persistent_decode_batch_chunks if self.persistent_decode_enabled else None,
            "persistent_decode_batch_chunks_configured": self.persistent_decode_batch_chunks if self.persistent_decode_enabled else None,
            "persistent_decode_batch_collect_timeout_seconds": self.persistent_decode_batch_collect_timeout_seconds if self.persistent_decode_enabled else None,
            "decoder_request_count": persistent_decoder_request_count,
            "batch_count": persistent_decoder_request_count,
            "persistent_decode_batch_count": persistent_decoder_request_count,
            "persistent_decode_multi_chunk_batch_count": persistent_multi_chunk_batch_count,
            "singleton_batch_count": persistent_singleton_batch_count,
            "partial_batch_count": persistent_partial_batch_count,
            "persistent_decode_singleton_batch_count": persistent_singleton_batch_count,
            "persistent_decode_partial_batch_count": persistent_partial_batch_count,
            "persistent_decode_batch_size": metric_stats(persistent_batch_sizes),
            "actual_batch_size_chunks": metric_stats(persistent_actual_batch_sizes),
            "persistent_decode_actual_batch_size": metric_stats(persistent_actual_batch_sizes),
            "persistent_decode_batch_request_response_seconds": metric_stats(persistent_batch_request_times),
            "persistent_decode_batch_worker_processing_seconds": metric_stats(persistent_batch_processing_times),
            "persistent_decode_batch_records": persistent_batch_records,
            "persistent_decode_batching_accounting": persistent_batching_accounting,
            "every_chunk_submitted_exactly_once": persistent_batching_accounting.get("every_chunk_submitted_exactly_once"),
            "every_result_returned_exactly_once": persistent_batching_accounting.get("every_result_returned_exactly_once"),
            "ordered_output_includes_every_chunk_exactly_once": persistent_batching_accounting.get("ordered_output_includes_every_chunk_exactly_once"),
            "persistent_decode_worker_failures": len(self.persistent_decode_worker_failures),
            "persistent_decode_fallback_used": self.persistent_decode_fallback_used,
            "write_append_time_seconds": metric_stats(append_times),
            "ordered_merge_time_seconds": metric_stats(merge_times),
            "ordered_merge_request_response_seconds": metric_stats(merge_request_times),
            "ordered_merge_count": len(self.output_merge_records),
            "validation_time_seconds": validation_elapsed,
            "visualization_update_time_seconds": visualization_elapsed_seconds,
            "capture_to_output_latency_seconds": metric_stats(latencies),
            "total_runtime_seconds": total_runtime_seconds,
            "validated_output_throughput": {
                "validation_pass_required": True,
                "validated_packets": validated_packets,
                "validated_rows": validated_rows,
                "measurement_window_seconds": total_runtime_seconds,
                "packets_per_second": throughput_packets,
                "rows_per_second": throughput_rows,
                "input_rate_pps": input_rate_pps,
                "keeps_up_with_input_rate": (
                    bool(validation_pass)
                    and throughput_packets is not None
                    and input_rate_pps is not None
                    and throughput_packets >= input_rate_pps
                ),
            },
            "output_paths": self.metric_output_paths(),
            "missing_metrics": missing,
            "visualization": visualization,
        }
        required_metric_gaps = self.required_metric_gaps(summary)
        summary["required_metric_gaps"] = required_metric_gaps
        for gap in required_metric_gaps:
            missing_label = f"required metric missing/null: {gap}"
            if missing_label not in summary["missing_metrics"]:
                summary["missing_metrics"].append(missing_label)
        return summary

    def build_pass_criteria(self, measurement: dict[str, Any]) -> dict[str, Any]:
        sent_packets = int_or_zero(measurement.get("sent_packets"))
        captured_packets = int_or_zero(measurement.get("captured_packets"))
        decoded_packets = int_or_zero(measurement.get("decoded_packets"))
        tcpdump_drops = int_or_zero(measurement.get("tcpdump_drops"))
        final_queue_depth = int_or_zero(measurement.get("final_queue_depth_chunks"))
        append_backlog = int_or_zero(measurement.get("append_backlog_chunks"))
        latency = measurement.get("capture_to_output_latency_seconds") or {}
        throughput = measurement.get("validated_output_throughput") or {}
        missing_metrics = list(measurement.get("missing_metrics") or [])
        expected_workers = (self.config.get("experiment") or {}).get("decoder_workers")
        replay_config = self.config.get("pcap_replay") or {}
        bounded_replay = self.source_mode == "pcap_replay" and replay_config.get("max_replay_chunks") not in (None, "")
        if bounded_replay:
            throughput_criterion = {
                "name": "validated output throughput recorded separately from input rate",
                "pass": throughput.get("packets_per_second") is not None and throughput.get("input_rate_pps") is not None,
                "values": {**throughput, "finite_burst_not_sustained_claim": True},
            }
        else:
            throughput_criterion = {
                "name": "validated output throughput keeps up with input rate",
                "pass": throughput.get("keeps_up_with_input_rate") is True,
                "values": throughput,
            }

        if self.source_mode == "pcap_replay":
            packet_counts_match = sent_packets > 0 and sent_packets == captured_packets and captured_packets == decoded_packets
            packet_count_name = "sent packets = captured packets = decoded packets"
        else:
            packet_counts_match = captured_packets > 0 and captured_packets == decoded_packets
            packet_count_name = "captured packets = decoded packets"

        criteria = [
            {
                "name": packet_count_name,
                "pass": packet_counts_match,
                "values": {
                    "sent_packets": sent_packets if self.source_mode == "pcap_replay" else None,
                    "captured_packets": captured_packets,
                    "decoded_packets": decoded_packets,
                },
            },
            {
                "name": "validation passes",
                "pass": measurement.get("validation_pass") is True,
                "values": {"validation_pass": measurement.get("validation_pass")},
            },
            {
                "name": "tcpdump drops = 0",
                "pass": tcpdump_drops == 0,
                "values": {"tcpdump_drops": tcpdump_drops},
            },
            {
                "name": "final queue depth = 0",
                "pass": final_queue_depth == 0 and append_backlog == 0,
                "values": {
                    "final_queue_depth_chunks": final_queue_depth,
                    "append_backlog_chunks": append_backlog,
                    "queue_returned_to_zero": measurement.get("queue_returned_to_zero"),
                },
            },
            {
                "name": "bounded queues enabled",
                "pass": measurement.get("bounded_queues") is True,
                "values": {
                    "bounded_queues": measurement.get("bounded_queues"),
                    "queue_capacity_chunks": measurement.get("queue_capacity_chunks"),
                },
            },
            {
                "name": "capture-to-output latency recorded",
                "pass": int_or_zero(latency.get("count")) > 0 and latency.get("max") is not None,
                "values": latency,
            },
            throughput_criterion,
            {
                "name": "no missing instrumentation",
                "pass": not missing_metrics,
                "values": {"missing_metrics": missing_metrics},
            },
        ]
        if expected_workers not in (None, ""):
            criteria.insert(
                0,
                {
                    "name": "decoder workers match experiment",
                    "pass": int_or_zero(measurement.get("decoder_workers")) == int(expected_workers),
                    "values": {
                        "decoder_workers": measurement.get("decoder_workers"),
                        "expected_decoder_workers": int(expected_workers),
                    },
                },
            )
        failure_point = next((item["name"] for item in criteria if not item["pass"]), None)
        return {
            "pass": failure_point is None,
            "failure_point": failure_point,
            "criteria": criteria,
        }

    def measurement_csv_row(self, summary: dict[str, Any]) -> dict[str, Any]:
        measurement = summary.get("measurement_summary") or {}
        pass_criteria = summary.get("pass_criteria") or measurement.get("pass_criteria") or {}
        throughput = measurement.get("validated_output_throughput") or {}
        capture_latency = measurement.get("capture_to_output_latency_seconds") or {}
        capture_time = measurement.get("capture_time_seconds") or {}
        decode_time = measurement.get("decode_time_seconds") or {}
        append_time = measurement.get("write_append_time_seconds") or {}
        merge_time = measurement.get("ordered_merge_time_seconds") or {}
        merge_request = measurement.get("ordered_merge_request_response_seconds") or {}
        worker_startup = measurement.get("persistent_decode_worker_startup_seconds") or {}
        worker_request = measurement.get("persistent_decode_request_response_seconds") or {}
        worker_processing = measurement.get("persistent_decode_worker_processing_seconds") or {}
        batch_size = measurement.get("persistent_decode_batch_size") or {}
        actual_batch_size = measurement.get("persistent_decode_actual_batch_size") or batch_size
        batch_request = measurement.get("persistent_decode_batch_request_response_seconds") or {}
        batch_processing = measurement.get("persistent_decode_batch_worker_processing_seconds") or {}
        return {
            "run_id": summary.get("run_id"),
            "pass": summary.get("pass"),
            "failure_point": pass_criteria.get("failure_point"),
            "event_id": measurement.get("event_id"),
            "decode_worker_mode": measurement.get("decode_worker_mode"),
            "output_write_strategy": measurement.get("output_write_strategy"),
            "input_rate_pps": measurement.get("input_rate_pps"),
            "packets_per_repeat": measurement.get("packets_per_repeat"),
            "chunk_packets": measurement.get("chunk_packets"),
            "decoder_workers": measurement.get("decoder_workers"),
            "queue_capacity_chunks": measurement.get("queue_capacity_chunks"),
            "bounded_queues": measurement.get("bounded_queues"),
            "sent_packets": measurement.get("sent_packets"),
            "captured_packets": measurement.get("captured_packets"),
            "decoded_packets": measurement.get("decoded_packets"),
            "decoded_rows": measurement.get("decoded_rows"),
            "validation_pass": measurement.get("validation_pass"),
            "tcpdump_drops": measurement.get("tcpdump_drops"),
            "max_queue_depth_chunks": measurement.get("max_queue_depth_chunks"),
            "final_queue_depth_chunks": measurement.get("final_queue_depth_chunks"),
            "queue_returned_to_zero": measurement.get("queue_returned_to_zero"),
            "backpressure_blocked_put_count": measurement.get("backpressure_blocked_put_count"),
            "backpressure_blocking_seconds": measurement.get("backpressure_blocking_seconds"),
            "writer_queue_depth_chunks": measurement.get("writer_queue_depth_chunks"),
            "writer_queue_max_depth_after_put_chunks": measurement.get("writer_queue_max_depth_after_put_chunks"),
            "writer_queue_blocked_put_count": measurement.get("writer_queue_blocked_put_count"),
            "writer_queue_blocking_seconds": measurement.get("writer_queue_blocking_seconds"),
            "capture_time_total_seconds": capture_time.get("total"),
            "decode_time_total_seconds": decode_time.get("total"),
            "persistent_decode_worker_startup_total_seconds": worker_startup.get("total"),
            "persistent_decode_request_response_total_seconds": worker_request.get("total"),
            "persistent_decode_worker_processing_total_seconds": worker_processing.get("total"),
            "requested_batch_size_chunks": measurement.get("requested_batch_size_chunks"),
            "persistent_decode_batch_chunks_configured": measurement.get("persistent_decode_batch_chunks_configured"),
            "decoder_request_count": measurement.get("decoder_request_count"),
            "batch_count": measurement.get("batch_count"),
            "persistent_decode_batch_count": measurement.get("persistent_decode_batch_count"),
            "persistent_decode_multi_chunk_batch_count": measurement.get("persistent_decode_multi_chunk_batch_count"),
            "singleton_batch_count": measurement.get("singleton_batch_count"),
            "partial_batch_count": measurement.get("partial_batch_count"),
            "persistent_decode_batch_size_min": batch_size.get("min"),
            "persistent_decode_batch_size_max": batch_size.get("max"),
            "persistent_decode_batch_size_mean": batch_size.get("mean"),
            "persistent_decode_batch_size_p50": batch_size.get("p50"),
            "persistent_decode_batch_size_p95": batch_size.get("p95"),
            "actual_batch_size_chunks_min": actual_batch_size.get("min"),
            "actual_batch_size_chunks_mean": actual_batch_size.get("mean"),
            "actual_batch_size_chunks_p50": actual_batch_size.get("p50"),
            "actual_batch_size_chunks_p95": actual_batch_size.get("p95"),
            "actual_batch_size_chunks_max": actual_batch_size.get("max"),
            "persistent_decode_actual_batch_size_max": actual_batch_size.get("max"),
            "persistent_decode_actual_batch_size_mean": actual_batch_size.get("mean"),
            "every_chunk_submitted_exactly_once": measurement.get("every_chunk_submitted_exactly_once"),
            "every_result_returned_exactly_once": measurement.get("every_result_returned_exactly_once"),
            "ordered_output_includes_every_chunk_exactly_once": measurement.get("ordered_output_includes_every_chunk_exactly_once"),
            "persistent_decode_batch_request_response_total_seconds": batch_request.get("total"),
            "persistent_decode_batch_worker_processing_total_seconds": batch_processing.get("total"),
            "persistent_decode_worker_failures": measurement.get("persistent_decode_worker_failures"),
            "persistent_decode_fallback_used": measurement.get("persistent_decode_fallback_used"),
            "write_append_time_total_seconds": append_time.get("total"),
            "ordered_merge_time_total_seconds": merge_time.get("total"),
            "ordered_merge_time_max_seconds": merge_time.get("max"),
            "ordered_merge_request_response_total_seconds": merge_request.get("total"),
            "ordered_merge_count": measurement.get("ordered_merge_count"),
            "validation_time_seconds": measurement.get("validation_time_seconds"),
            "capture_to_output_latency_p50_seconds": capture_latency.get("p50"),
            "capture_to_output_latency_p95_seconds": capture_latency.get("p95"),
            "capture_to_output_latency_max_seconds": capture_latency.get("max"),
            "total_runtime_seconds": measurement.get("total_runtime_seconds"),
            "validated_output_packets_per_second": throughput.get("packets_per_second"),
            "validated_output_rows_per_second": throughput.get("rows_per_second"),
            "validated_output_keeps_up_with_input_rate": throughput.get("keeps_up_with_input_rate"),
            "missing_metrics": ",".join(measurement.get("missing_metrics") or []),
        }

    def write_local_measurement_files(self, summary: dict[str, Any]) -> None:
        write_csv_rows(self.layout.queue_history_csv, list(self.queue_metrics_history))
        write_csv_rows(self.layout.measurement_csv, [self.measurement_csv_row(summary)])

    def run(self, *, dry_run: bool = False, emit_json: bool = True) -> dict[str, Any]:
        self.validate_config()
        if dry_run:
            return self.dry_run_payload(emit_json=emit_json)
        run_started_monotonic = time.monotonic()
        self.manifest["run_start_time"] = now_iso()
        self.prepare_local_dirs()
        self.write_manifest(status="running", phase="preflight")
        error: Exception | None = None
        summary: dict[str, Any] = {}
        try:
            self.preflight()
            self.prepare_remote()
            self.write_manifest(status="running", phase="input_capture")
            if self.source_mode == "pcap_replay":
                if self.pipeline_mode == "concurrent":
                    self.run_pcap_replay_concurrent()
                else:
                    self.run_pcap_replay()
            else:
                if self.pipeline_mode == "concurrent":
                    self.run_live_nic_concurrent()
                else:
                    self.run_live_nic()
            self.write_manifest(status="running", phase="validation")
            validation = self.run_validation()
            self.write_manifest(status="running", phase="download")
            self.download_outputs()
            self.write_manifest(status="running", phase="visualization")
            visualization_start = time.monotonic()
            viz = self.render_visualizations()
            visualization_elapsed_seconds = round(time.monotonic() - visualization_start, 3)
            queue_metrics = self.queue_metrics()
            total_runtime_seconds = round(time.monotonic() - run_started_monotonic, 3)
            measurement_summary = self.build_measurement_summary(
                validation=validation,
                visualization=viz,
                total_runtime_seconds=total_runtime_seconds,
                visualization_elapsed_seconds=visualization_elapsed_seconds,
            )
            self.assert_required_metrics_present(measurement_summary)
            pass_criteria = self.build_pass_criteria(measurement_summary)
            measurement_summary["pass_criteria"] = pass_criteria
            measurement_summary["failure_point"] = pass_criteria["failure_point"]
            run_pass = bool(pass_criteria["pass"])
            summary = {
                "pass": run_pass,
                "failure_point": pass_criteria["failure_point"],
                "pass_criteria": pass_criteria,
                "run_id": self.layout.run_id,
                "source_mode": self.source_mode,
                "pipeline_mode": self.pipeline_mode,
                "decode_worker_mode": self.decode_worker_mode,
                "manifest": str(self.layout.manifest_json),
                "combined_csv": str(self.layout.local_combined_csv),
                "visualization": viz,
                "validation": validation,
                "queue_metrics": queue_metrics,
                "queue_metrics_history": list(self.queue_metrics_history),
                "overlap_metrics": self.manifest.get("overlap_metrics"),
                "decode_workers_configured": self.decode_workers,
                "decode_worker_assignments": list(self.decode_worker_assignments),
                "persistent_decode_workers": copy.deepcopy(self.persistent_decode_workers),
                "persistent_decode_worker_failures": copy.deepcopy(self.persistent_decode_worker_failures),
                "persistent_decode_batches": copy.deepcopy(self.persistent_decode_batches),
                "persistent_decode_result_records": copy.deepcopy(self.persistent_decode_result_records),
                "persistent_decode_fallback_used": self.persistent_decode_fallback_used,
                "ordered_output_records": copy.deepcopy(self.ordered_output_records),
                "chunk_delivery_accounting": self.chunk_delivery_accounting(),
                "max_queue_depth_chunks": queue_metrics.get("max_queue_depth_chunks"),
                "final_queue_depth_chunks": queue_metrics.get("final_queue_depth_chunks"),
                "total_runtime_seconds": total_runtime_seconds,
                "measurement_summary": measurement_summary,
                "measurement_csv": str(self.layout.measurement_csv),
                "queue_metrics_history_csv": str(self.layout.queue_history_csv),
                "validation_json": str(self.layout.validation_json),
                "report_mirror_dir": str(self.layout.report_mirror_dir) if self.layout.report_mirror_dir else None,
            }
            decoded_chunk_mirroring = self.mirror_decoded_chunks_for_reporting()
            summary["decoded_chunk_mirroring"] = decoded_chunk_mirroring
            self.manifest["validation_result"] = validation
            self.manifest["visualization"] = viz
            self.manifest["measurement_summary"] = measurement_summary
            self.manifest["pass_criteria"] = pass_criteria
            self.manifest["failure_point"] = pass_criteria["failure_point"]
            self.manifest["pass"] = run_pass
            self.manifest["total_runtime_seconds"] = total_runtime_seconds
            self.write_manifest(status="complete", phase="complete")
        except Exception as exc:
            error = exc
            try:
                self.stop_persistent_decode_workers()
            except Exception as stop_exc:
                self.persistent_decode_worker_failures.append(
                    {
                        "phase": "exception_cleanup",
                        "error": str(stop_exc),
                    }
                )
            queue_metrics = self.queue_metrics()
            total_runtime_seconds = round(time.monotonic() - run_started_monotonic, 3)
            validation = {"enabled": self.validation_enabled, "pass": False, "error": str(exc)}
            measurement_summary = self.build_measurement_summary(
                validation=validation,
                visualization=None,
                total_runtime_seconds=total_runtime_seconds,
                visualization_elapsed_seconds=None,
            )
            pass_criteria = self.build_pass_criteria(measurement_summary)
            measurement_summary["pass_criteria"] = pass_criteria
            measurement_summary["failure_point"] = pass_criteria["failure_point"]
            summary = {
                "pass": False,
                "failure_point": pass_criteria["failure_point"],
                "pass_criteria": pass_criteria,
                "run_id": self.layout.run_id,
                "source_mode": self.source_mode,
                "pipeline_mode": self.pipeline_mode,
                "error": str(exc),
                "manifest": str(self.layout.manifest_json),
                "validation": validation,
                "queue_metrics": queue_metrics,
                "decode_workers_configured": self.decode_workers,
                "decode_worker_mode": self.decode_worker_mode,
                "decode_worker_assignments": list(self.decode_worker_assignments),
                "persistent_decode_workers": copy.deepcopy(self.persistent_decode_workers),
                "persistent_decode_worker_failures": copy.deepcopy(self.persistent_decode_worker_failures),
                "persistent_decode_batches": copy.deepcopy(self.persistent_decode_batches),
                "persistent_decode_result_records": copy.deepcopy(self.persistent_decode_result_records),
                "persistent_decode_fallback_used": self.persistent_decode_fallback_used,
                "ordered_output_records": copy.deepcopy(self.ordered_output_records),
                "chunk_delivery_accounting": self.chunk_delivery_accounting(),
                "max_queue_depth_chunks": queue_metrics.get("max_queue_depth_chunks"),
                "final_queue_depth_chunks": queue_metrics.get("final_queue_depth_chunks"),
                "total_runtime_seconds": total_runtime_seconds,
                "measurement_summary": measurement_summary,
                "measurement_csv": str(self.layout.measurement_csv),
                "queue_metrics_history_csv": str(self.layout.queue_history_csv),
                "validation_json": str(self.layout.validation_json),
                "report_mirror_dir": str(self.layout.report_mirror_dir) if self.layout.report_mirror_dir else None,
            }
            self.manifest["measurement_summary"] = measurement_summary
            self.manifest["pass_criteria"] = pass_criteria
            self.manifest["failure_point"] = pass_criteria["failure_point"]
            self.write_manifest(status="failed", phase="failed", error=str(exc))
        finally:
            summary.setdefault("config_path", str(self.config_path))
            summary.setdefault("local_run_dir", str(self.layout.local_run_dir))
            summary.setdefault("validation_json", str(self.layout.validation_json))
            summary.setdefault("report_mirror_dir", str(self.layout.report_mirror_dir) if self.layout.report_mirror_dir else None)
            self.write_report_bundle(summary)
            mirrored = self.mirror_current_reports()
            if mirrored:
                summary["report_mirror"] = {
                    "report_dir": str(self.layout.report_mirror_dir),
                    "files": mirrored,
                }
                self.write_report_bundle(summary)
                self.mirror_current_reports()
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
        if self.persistent_decode_enabled:
            self.ensure_receiver_helper(RECEIVER_DECODE_WORKER, self.layout.remote_decode_worker)
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

    def start_concurrent_pipeline_threads(
        self,
        work_queue: queue.Queue[dict[str, Any] | None],
        decoded_queue: queue.Queue[dict[str, Any] | None],
        errors: list[BaseException],
        producer_done: threading.Event,
    ) -> tuple[list[threading.Thread], threading.Thread]:
        self.start_persistent_decode_workers()
        workers: list[threading.Thread] = []
        for worker_id in range(1, self.decode_workers + 1):
            worker = threading.Thread(
                target=self.concurrent_decode_worker,
                args=(worker_id, work_queue, decoded_queue, errors),
                name=f"{self.layout.run_id}-decode-worker-{worker_id}",
                daemon=True,
            )
            worker.start()
            workers.append(worker)
        append_thread = threading.Thread(
            target=self.concurrent_ordered_append_coordinator,
            args=(producer_done, decoded_queue, errors),
            name=f"{self.layout.run_id}-ordered-append-coordinator",
            daemon=True,
        )
        append_thread.start()
        return workers, append_thread

    def finish_concurrent_pipeline_threads(
        self,
        work_queue: queue.Queue[dict[str, Any] | None],
        errors: list[BaseException],
        producer_done: threading.Event,
        workers: list[threading.Thread],
        append_thread: threading.Thread,
        produced: int,
    ) -> None:
        stop_error: BaseException | None = None
        try:
            producer_done.set()
            with self._chunk_condition:
                self._chunk_condition.notify_all()
            for _ in workers:
                while True:
                    try:
                        work_queue.put(None, timeout=1.0)
                        break
                    except queue.Full:
                        if errors:
                            break
            join_timeout = max(self.timeout, self.timeout * max(1, produced))
            for worker in workers:
                worker.join(timeout=join_timeout)
            alive_workers = [worker.name for worker in workers if worker.is_alive()]
            if alive_workers:
                raise PipelineError(f"concurrent decode workers did not finish before timeout: {alive_workers}")
            append_thread.join(timeout=join_timeout)
            if append_thread.is_alive():
                raise PipelineError("ordered append coordinator did not finish before timeout")
        finally:
            try:
                self.stop_persistent_decode_workers()
            except BaseException as error:
                stop_error = error
        if errors:
            raise PipelineError(f"concurrent pipeline failed: {errors[0]}")
        if stop_error is not None:
            raise PipelineError(f"concurrent pipeline cleanup failed: {stop_error}")

    def run_pcap_replay_concurrent(self) -> None:
        split = self.remote_exec(self.sender_node, self.split_source_pcap_command(), 120, True)
        source_manifest = parse_sentinel_json(split.stdout, "CHUNK_MANIFEST:")
        chunks = source_manifest.get("chunks") or []
        if not chunks:
            raise PipelineError("sender PCAP splitter produced no chunks")
        self.manifest["source_manifest"] = source_manifest
        self.manifest["concurrent_pipeline"] = {
            "enabled": True,
            "producer": "pcap_replay_capture",
            "decode_workers": self.decode_workers,
            "ordered_output_writer": self.output_write_strategy,
            "ordered_append": (
                "single deterministic merge of decoded chunk CSVs after all chunks are ready"
                if self.output_write_strategy == "ordered_chunk_merge"
                else "single coordinator appends decoded chunks to combined.csv strictly by chunk_index"
            ),
            "raw_chunk_completion": "tcpdump writes .partial, then the producer atomically renames to .pcap and writes a .ready marker",
        }
        self.write_manifest(status="running", phase="concurrent_capture_decode")

        work_queue = self.new_pipeline_queue()
        decoded_queue = self.new_pipeline_queue()
        errors: list[BaseException] = []
        producer_done = threading.Event()
        workers, append_thread = self.start_concurrent_pipeline_threads(work_queue, decoded_queue, errors, producer_done)
        producer_start = time.monotonic()
        self.manifest["producer_start_time"] = now_iso()
        produced = 0
        for chunk in chunks:
            if errors:
                break
            entry = self.capture_one_pcap_raw_chunk(chunk)
            produced += 1
            self.put_with_backpressure(
                work_queue,
                entry,
                "raw_to_decode",
                chunk_index=int(entry.get("chunk_index", -1)),
            )
            self.record_queue_metrics_snapshot(status="running", phase="concurrent_capture_decode")
        self.manifest["producer_complete_time"] = now_iso()
        self.manifest["producer_elapsed_seconds"] = round(time.monotonic() - producer_start, 3)
        self.manifest["raw_chunks_produced_by_producer"] = produced
        self.finish_concurrent_pipeline_threads(work_queue, errors, producer_done, workers, append_thread, produced)
        self.finalize_deferred_capture_accounting()
        self.add_overlap_metrics()
        self.write_manifest(status="running", phase="concurrent_capture_decode_complete")

    def run_live_nic_concurrent(self) -> None:
        live = self.config.get("live_nic", {})
        max_chunks = live.get("max_chunks")
        if max_chunks in (None, "") and live.get("require_follow_for_unbounded", True) and not self.follow:
            raise PipelineError("live_nic has no max_chunks; pass --follow for an unbounded run or set --max-live-chunks")
        stop_file = str(live.get("stop_file") or "/tmp/iceberg_fabric.stop")
        self.manifest["concurrent_pipeline"] = {
            "enabled": True,
            "producer": "live_nic_capture",
            "decode_workers": self.decode_workers,
            "ordered_output_writer": self.output_write_strategy,
            "ordered_append": (
                "single deterministic merge of decoded chunk CSVs after all chunks are ready"
                if self.output_write_strategy == "ordered_chunk_merge"
                else "single coordinator appends decoded chunks to combined.csv strictly by chunk_index"
            ),
            "raw_chunk_completion": "tcpdump writes .partial, then the producer atomically renames to .pcap and writes a .ready marker",
        }
        self.write_manifest(status="running", phase="concurrent_capture_decode")

        work_queue = self.new_pipeline_queue()
        decoded_queue = self.new_pipeline_queue()
        errors: list[BaseException] = []
        producer_done = threading.Event()
        workers, append_thread = self.start_concurrent_pipeline_threads(work_queue, decoded_queue, errors, producer_done)
        producer_start = time.monotonic()
        self.manifest["producer_start_time"] = now_iso()
        produced = 0
        index = 0
        while True:
            if errors:
                break
            if max_chunks not in (None, "") and index >= int(max_chunks):
                break
            stop_check = self.remote_exec(self.receiver_node, f"test -f {shlex.quote(stop_file)}", 15, False)
            if stop_check.returncode == 0:
                break
            entry = self.capture_one_live_raw_chunk(index)
            produced += 1
            self.put_with_backpressure(
                work_queue,
                entry,
                "raw_to_decode",
                chunk_index=int(entry.get("chunk_index", -1)),
            )
            self.record_queue_metrics_snapshot(status="running", phase="concurrent_capture_decode")
            index += 1
        self.manifest["producer_complete_time"] = now_iso()
        self.manifest["producer_elapsed_seconds"] = round(time.monotonic() - producer_start, 3)
        self.manifest["raw_chunks_produced_by_producer"] = produced
        self.finish_concurrent_pipeline_threads(work_queue, errors, producer_done, workers, append_thread, produced)
        self.finalize_deferred_capture_accounting()
        self.add_overlap_metrics()
        self.write_manifest(status="running", phase="concurrent_capture_decode_complete")

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
        stop_file = str(live.get("stop_file") or "/tmp/iceberg_fabric.stop")
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
        local_chunk_csv = self.local_decoded_chunk_path(index)
        decode_handoff_at = now_iso()
        append_result = self.remote_exec(self.receiver_node, self.append_command(index, remote_csv), 120, True)
        combined_lines = parse_last_int(append_result.stdout)
        cumulative_rows = max(0, combined_lines - 1)
        chunk_entry.update(
            {
                "status": "appended",
                "decode_status": "decoded",
                "append_status": "appended",
                "decoded_csv_path": remote_csv,
                "local_decoded_csv_path": str(local_chunk_csv),
                "local_decoded_csv_size": None,
                "local_decoded_csv_status": "copy_deferred",
                "decoded_chunk_copy_deferred": True,
                "decoded_chunk_copy_phase": "post_live_reporting",
                "decoded_result_handoff_time": decode_handoff_at,
                "decode_started_at": decode_started_at,
                "decode_started_marker_path": decode_started_marker,
                "decode_completed_at": decode_completed_at,
                "decode_completed_time": decode_completed_at,
                "decode_completed_marker_path": decode_completed_marker,
                "local_chunk_csv_available_at": None,
                "local_copy_completed_time": None,
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
            return {
                "enabled": False,
                "pass": None,
                "reason": "validation disabled or no reference CSV configured",
                "validation_elapsed_seconds": 0.0,
            }

        validation_started_at = now_iso()
        validation_start = time.monotonic()
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
        report["validation_started_at"] = validation_started_at
        report["validation_completed_at"] = now_iso()
        report["validation_elapsed_seconds"] = round(time.monotonic() - validation_start, 3)
        report["validation_time_seconds"] = report["validation_elapsed_seconds"]
        report.setdefault("returncode", result.returncode)
        self.write_remote_json(self.receiver_node, report_path, report, 60)
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
    root_value = output_root or Path(str(config.get("paths", {}).get("output_root") or "/tmp/iceberg_fabric_runs"))
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
        "sent_packets": 0,
        "sender_failed_packets": 0,
        "captured_packets": 0,
        "decoded_packets": 0,
        "decoded_rows": 0,
        "validation_pass_count": 0,
        "validation_fail_count": 0,
        "missing_metric_repeat_count": 0,
        "missing_metrics": [],
        "tcpdump_drops": 0,
        "max_queue_depth_chunks": 0,
        "final_queue_depth_chunks": 0,
        "max_decode_lag_chunks": 0,
        "max_decode_lag_seconds": 0.0,
        "capture_decode_overlap_seconds": 0.0,
        "backpressure_blocked_put_count": 0,
        "backpressure_blocking_seconds": 0.0,
        "writer_queue_max_depth_after_put_chunks": 0,
        "writer_queue_blocked_put_count": 0,
        "writer_queue_blocking_seconds": 0.0,
        "max_ordered_write_buffer_depth_chunks": 0,
        "capture_time_seconds": 0.0,
        "decode_time_seconds": 0.0,
        "write_append_time_seconds": 0.0,
        "ordered_merge_time_seconds": 0.0,
        "ordered_merge_request_response_seconds": 0.0,
        "ordered_merge_count": 0,
        "validation_time_seconds": 0.0,
        "persistent_decode_worker_startup_seconds": 0.0,
        "persistent_decode_request_response_seconds": 0.0,
        "persistent_decode_worker_processing_seconds": 0.0,
        "requested_batch_size_chunks": [],
        "decoder_request_count": 0,
        "batch_count": 0,
        "persistent_decode_batch_count": 0,
        "persistent_decode_multi_chunk_batch_count": 0,
        "singleton_batch_count": 0,
        "partial_batch_count": 0,
        "persistent_decode_batch_max_size": 0,
        "persistent_decode_batch_size_total": 0.0,
        "persistent_decode_batch_mean_size": 0.0,
        "actual_batch_size_chunks": metric_stats([]),
        "persistent_decode_batch_request_response_seconds": 0.0,
        "persistent_decode_batch_worker_processing_seconds": 0.0,
        "persistent_decode_worker_failures": 0,
        "persistent_decode_fallback_used": False,
        "total_runtime_seconds": 0.0,
        "capture_to_output_latency_seconds": {
            "count": 0,
            "max": None,
            "per_repeat_max": [],
        },
        "event_id": [],
        "decode_worker_mode": [],
        "output_write_strategy": [],
        "packets_per_repeat": [],
        "chunk_packets": [],
        "decode_workers_configured": [],
        "queue_capacity_chunks": [],
        "bounded_queues_confirmed": None,
        "input_rate_pps": [],
        "total_rows": 0,
        "required_metric_fields_emitted": None,
        "required_metric_gaps": [],
    }
    all_bounded: list[bool] = []
    actual_batch_size_values: list[float] = []
    submitted_once_values: list[bool] = []
    returned_once_values: list[bool] = []
    ordered_once_values: list[bool] = []
    for entry in repeats:
        metrics = entry.get("queue_metrics") or {}
        measurement = entry.get("measurement_summary") or {}
        totals["raw_chunk_preserved_count"] += int_metric(metrics, "raw_chunk_preserved_count")
        totals["decode_completed_chunk_count"] += int_metric(metrics, "decode_completed_chunk_count")
        totals["append_completed_chunk_count"] += int_metric(metrics, "append_completed_chunk_count")
        totals["raw_packet_drops"] += int_metric(metrics, "raw_packet_drops")
        event_id = measurement.get("event_id")
        if event_id not in (None, "") and event_id not in totals["event_id"]:
            totals["event_id"].append(event_id)
        decode_worker_mode = measurement.get("decode_worker_mode")
        if decode_worker_mode not in (None, "") and decode_worker_mode not in totals["decode_worker_mode"]:
            totals["decode_worker_mode"].append(decode_worker_mode)
        output_write_strategy = measurement.get("output_write_strategy")
        if output_write_strategy not in (None, "") and output_write_strategy not in totals["output_write_strategy"]:
            totals["output_write_strategy"].append(output_write_strategy)
        packets_per_repeat = measurement.get("packets_per_repeat")
        if packets_per_repeat not in (None, "") and packets_per_repeat not in totals["packets_per_repeat"]:
            totals["packets_per_repeat"].append(packets_per_repeat)
        chunk_packets = measurement.get("chunk_packets")
        if chunk_packets not in (None, "") and chunk_packets not in totals["chunk_packets"]:
            totals["chunk_packets"].append(chunk_packets)
        workers = measurement.get("decoder_workers", metrics.get("decode_workers_configured"))
        if workers not in (None, "") and workers not in totals["decode_workers_configured"]:
            totals["decode_workers_configured"].append(workers)
        capacity = measurement.get("queue_capacity_chunks", metrics.get("queue_capacity_chunks"))
        if capacity not in (None, "") and capacity not in totals["queue_capacity_chunks"]:
            totals["queue_capacity_chunks"].append(capacity)
        if measurement.get("bounded_queues") is not None:
            all_bounded.append(bool(measurement.get("bounded_queues")))
        rate = measurement.get("input_rate_pps")
        if rate not in (None, "") and rate not in totals["input_rate_pps"]:
            totals["input_rate_pps"].append(rate)
        totals["max_queue_depth_chunks"] = max(
            totals["max_queue_depth_chunks"],
            int_metric(metrics, "queue_depth_chunks"),
            int_metric(metrics, "max_queue_depth_chunks"),
            int_or_zero(measurement.get("max_queue_depth_chunks")),
        )
        totals["final_queue_depth_chunks"] += int_or_zero(measurement.get("final_queue_depth_chunks"))
        totals["sent_packets"] += int_or_zero(measurement.get("sent_packets"))
        totals["sender_failed_packets"] += int_or_zero(measurement.get("sender_failed_packets"))
        totals["captured_packets"] += int_or_zero(measurement.get("captured_packets"))
        totals["decoded_packets"] += int_or_zero(measurement.get("decoded_packets"))
        totals["decoded_rows"] += int_or_zero(measurement.get("decoded_rows"))
        totals["tcpdump_drops"] += int_or_zero(measurement.get("tcpdump_drops"))
        if measurement.get("validation_pass") is True:
            totals["validation_pass_count"] += 1
        elif measurement.get("validation_pass") is False:
            totals["validation_fail_count"] += 1
        repeat_missing = list(measurement.get("missing_metrics") or [])
        if repeat_missing:
            totals["missing_metric_repeat_count"] += 1
            for metric in repeat_missing:
                if metric not in totals["missing_metrics"]:
                    totals["missing_metrics"].append(metric)
        for gap in measurement.get("required_metric_gaps") or []:
            if gap not in totals["required_metric_gaps"]:
                totals["required_metric_gaps"].append(gap)
        totals["backpressure_blocked_put_count"] += int_or_zero(measurement.get("backpressure_blocked_put_count"))
        totals["backpressure_blocking_seconds"] += float(measurement.get("backpressure_blocking_seconds") or 0.0)
        totals["writer_queue_max_depth_after_put_chunks"] = max(
            totals["writer_queue_max_depth_after_put_chunks"],
            int_or_zero(measurement.get("writer_queue_max_depth_after_put_chunks")),
        )
        totals["writer_queue_blocked_put_count"] += int_or_zero(measurement.get("writer_queue_blocked_put_count"))
        totals["writer_queue_blocking_seconds"] += float(measurement.get("writer_queue_blocking_seconds") or 0.0)
        totals["max_ordered_write_buffer_depth_chunks"] = max(
            totals["max_ordered_write_buffer_depth_chunks"],
            int_metric(metrics, "max_ordered_write_buffer_depth_chunks"),
        )
        totals["capture_time_seconds"] += float((measurement.get("capture_time_seconds") or {}).get("total") or 0.0)
        totals["decode_time_seconds"] += float((measurement.get("decode_time_seconds") or {}).get("total") or 0.0)
        totals["persistent_decode_worker_startup_seconds"] += float(
            (measurement.get("persistent_decode_worker_startup_seconds") or {}).get("total") or 0.0
        )
        totals["persistent_decode_request_response_seconds"] += float(
            (measurement.get("persistent_decode_request_response_seconds") or {}).get("total") or 0.0
        )
        totals["persistent_decode_worker_processing_seconds"] += float(
            (measurement.get("persistent_decode_worker_processing_seconds") or {}).get("total") or 0.0
        )
        requested_batch_size = measurement.get("requested_batch_size_chunks", measurement.get("persistent_decode_batch_chunks_configured"))
        if requested_batch_size not in (None, "") and requested_batch_size not in totals["requested_batch_size_chunks"]:
            totals["requested_batch_size_chunks"].append(requested_batch_size)
        batch_size_stats = measurement.get("actual_batch_size_chunks") or measurement.get("persistent_decode_actual_batch_size") or measurement.get("persistent_decode_batch_size") or {}
        request_count = int_or_zero(measurement.get("decoder_request_count", measurement.get("persistent_decode_batch_count")))
        totals["decoder_request_count"] += request_count
        totals["batch_count"] += int_or_zero(measurement.get("batch_count", request_count))
        totals["persistent_decode_batch_count"] += int_or_zero(measurement.get("persistent_decode_batch_count"))
        totals["persistent_decode_multi_chunk_batch_count"] += int_or_zero(
            measurement.get("persistent_decode_multi_chunk_batch_count")
        )
        totals["singleton_batch_count"] += int_or_zero(
            measurement.get("singleton_batch_count", measurement.get("persistent_decode_singleton_batch_count"))
        )
        totals["partial_batch_count"] += int_or_zero(
            measurement.get("partial_batch_count", measurement.get("persistent_decode_partial_batch_count"))
        )
        totals["persistent_decode_batch_size_total"] += float(batch_size_stats.get("total") or 0.0)
        totals["persistent_decode_batch_max_size"] = max(
            totals["persistent_decode_batch_max_size"],
            int_or_zero(batch_size_stats.get("max")),
        )
        for batch in measurement.get("persistent_decode_batch_records") or entry.get("persistent_decode_batches") or []:
            batch_size_value = float_or_none(batch.get("actual_batch_size_chunks", batch.get("actual_batch_size", batch.get("batch_size"))))
            if batch_size_value is not None:
                actual_batch_size_values.append(batch_size_value)
        for key, sink in (
            ("every_chunk_submitted_exactly_once", submitted_once_values),
            ("every_result_returned_exactly_once", returned_once_values),
            ("ordered_output_includes_every_chunk_exactly_once", ordered_once_values),
        ):
            if measurement.get(key) is not None:
                sink.append(bool(measurement.get(key)))
        totals["persistent_decode_batch_request_response_seconds"] += float(
            (measurement.get("persistent_decode_batch_request_response_seconds") or {}).get("total") or 0.0
        )
        totals["persistent_decode_batch_worker_processing_seconds"] += float(
            (measurement.get("persistent_decode_batch_worker_processing_seconds") or {}).get("total") or 0.0
        )
        totals["persistent_decode_worker_failures"] += int_or_zero(measurement.get("persistent_decode_worker_failures"))
        totals["persistent_decode_fallback_used"] = bool(
            totals["persistent_decode_fallback_used"] or measurement.get("persistent_decode_fallback_used")
        )
        totals["write_append_time_seconds"] += float((measurement.get("write_append_time_seconds") or {}).get("total") or 0.0)
        totals["ordered_merge_time_seconds"] += float((measurement.get("ordered_merge_time_seconds") or {}).get("total") or 0.0)
        totals["ordered_merge_request_response_seconds"] += float(
            (measurement.get("ordered_merge_request_response_seconds") or {}).get("total") or 0.0
        )
        totals["ordered_merge_count"] += int_or_zero(measurement.get("ordered_merge_count"))
        totals["validation_time_seconds"] += float(measurement.get("validation_time_seconds") or 0.0)
        overlap = entry.get("overlap_metrics") or {}
        totals["capture_decode_overlap_seconds"] += float(overlap.get("capture_decode_overlap_seconds") or 0.0)
        totals["max_decode_lag_seconds"] = max(
            totals["max_decode_lag_seconds"],
            float(overlap.get("maximum_decode_lag_seconds") or 0.0),
        )
        totals["max_decode_lag_chunks"] = max(
            totals["max_decode_lag_chunks"],
            int(overlap.get("maximum_decode_lag_chunks") or 0),
        )
        totals["total_runtime_seconds"] += float(entry.get("total_runtime_seconds") or measurement.get("total_runtime_seconds") or 0.0)
        latency = measurement.get("capture_to_output_latency_seconds") or {}
        latency_count = int_or_zero(latency.get("count"))
        latency_max = float_or_none(latency.get("max"))
        totals["capture_to_output_latency_seconds"]["count"] += latency_count
        if latency_max is not None:
            totals["capture_to_output_latency_seconds"]["per_repeat_max"].append(latency_max)
            current_max = totals["capture_to_output_latency_seconds"]["max"]
            totals["capture_to_output_latency_seconds"]["max"] = latency_max if current_max is None else max(current_max, latency_max)
        for point in entry.get("queue_tracking") or []:
            totals["max_queue_depth_chunks"] = max(totals["max_queue_depth_chunks"], int(point.get("queue_depth_chunks") or 0))
            totals["max_decode_lag_chunks"] = max(totals["max_decode_lag_chunks"], int(point.get("decode_lag_chunks") or 0))
        totals["total_rows"] += int(entry.get("decoded_rows") or measurement.get("decoded_rows") or 0)
    if all_bounded:
        totals["bounded_queues_confirmed"] = all(all_bounded)
    totals["backpressure_blocking_seconds"] = round(totals["backpressure_blocking_seconds"], 6)
    totals["writer_queue_blocking_seconds"] = round(totals["writer_queue_blocking_seconds"], 6)
    if totals["persistent_decode_batch_count"]:
        totals["persistent_decode_batch_mean_size"] = round(
            totals["persistent_decode_batch_size_total"] / totals["persistent_decode_batch_count"],
            3,
        )
    if actual_batch_size_values:
        totals["actual_batch_size_chunks"] = metric_stats(actual_batch_size_values)
    if submitted_once_values:
        totals["every_chunk_submitted_exactly_once"] = all(submitted_once_values)
    if returned_once_values:
        totals["every_result_returned_exactly_once"] = all(returned_once_values)
    if ordered_once_values:
        totals["ordered_output_includes_every_chunk_exactly_once"] = all(ordered_once_values)
    totals["required_metric_fields_emitted"] = not totals["required_metric_gaps"]
    for key in (
        "capture_time_seconds",
        "decode_time_seconds",
        "persistent_decode_worker_startup_seconds",
        "persistent_decode_request_response_seconds",
        "persistent_decode_worker_processing_seconds",
        "persistent_decode_batch_request_response_seconds",
        "persistent_decode_batch_worker_processing_seconds",
        "write_append_time_seconds",
        "ordered_merge_time_seconds",
        "ordered_merge_request_response_seconds",
        "validation_time_seconds",
        "total_runtime_seconds",
    ):
        totals[key] = round(float(totals[key]), 3)
    all_repeats_are_dry_run = bool(repeats) and all(bool(entry.get("dry_run")) for entry in repeats)
    throughput_packets = None
    throughput_rows = None
    if totals["validation_fail_count"] == 0 and totals["total_runtime_seconds"] > 0:
        throughput_packets = round(totals["decoded_packets"] / totals["total_runtime_seconds"], 3)
        throughput_rows = round(totals["decoded_rows"] / totals["total_runtime_seconds"], 3)
    elif all_repeats_are_dry_run:
        throughput_packets = 0.0
        throughput_rows = 0.0
    input_rate = totals["input_rate_pps"][0] if len(totals["input_rate_pps"]) == 1 else None
    totals["validated_output_throughput"] = {
        "validation_pass_required": True,
        "validated_packets": totals["decoded_packets"] if totals["validation_fail_count"] == 0 else 0,
        "validated_rows": totals["decoded_rows"] if totals["validation_fail_count"] == 0 else 0,
        "measurement_window_seconds": totals["total_runtime_seconds"],
        "packets_per_second": throughput_packets,
        "rows_per_second": throughput_rows,
        "input_rate_pps": input_rate,
        "keeps_up_with_input_rate": (
            throughput_packets is not None and input_rate is not None and throughput_packets >= input_rate
        ),
        "dry_run_placeholder": all_repeats_are_dry_run,
    }
    criteria = [
        {
            "name": "all repeats completed",
            "pass": totals["repeat_count"] > 0
            and totals["failed_repeat_count"] == 0
            and totals["completed_repeat_count"] == totals["repeat_count"],
            "values": {
                "repeat_count": totals["repeat_count"],
                "completed_repeat_count": totals["completed_repeat_count"],
                "failed_repeat_count": totals["failed_repeat_count"],
            },
        },
        {
            "name": "sent packets = captured packets = decoded packets",
            "pass": totals["sent_packets"] > 0
            and totals["sent_packets"] == totals["captured_packets"]
            and totals["captured_packets"] == totals["decoded_packets"],
            "values": {
                "sent_packets": totals["sent_packets"],
                "captured_packets": totals["captured_packets"],
                "decoded_packets": totals["decoded_packets"],
            },
        },
        {
            "name": "validation passes",
            "pass": totals["repeat_count"] > 0
            and totals["validation_pass_count"] == totals["repeat_count"]
            and totals["validation_fail_count"] == 0,
            "values": {
                "validation_pass_count": totals["validation_pass_count"],
                "validation_fail_count": totals["validation_fail_count"],
            },
        },
        {
            "name": "tcpdump drops = 0",
            "pass": int_or_zero(totals.get("tcpdump_drops")) == 0,
            "values": {"tcpdump_drops": totals.get("tcpdump_drops")},
        },
        {
            "name": "final queue depth = 0",
            "pass": int_or_zero(totals.get("final_queue_depth_chunks")) == 0,
            "values": {"final_queue_depth_chunks": totals.get("final_queue_depth_chunks")},
        },
        {
            "name": "bounded queues enabled",
            "pass": totals.get("bounded_queues_confirmed") is True,
            "values": {"bounded_queues_confirmed": totals.get("bounded_queues_confirmed")},
        },
        {
            "name": "capture-to-output latency recorded",
            "pass": int_or_zero(totals["capture_to_output_latency_seconds"].get("count")) > 0
            and totals["capture_to_output_latency_seconds"].get("max") is not None,
            "values": totals["capture_to_output_latency_seconds"],
        },
        {
            "name": "validated output throughput keeps up with input rate",
            "pass": totals["validated_output_throughput"].get("keeps_up_with_input_rate") is True,
            "values": totals["validated_output_throughput"],
        },
        {
            "name": "no missing instrumentation",
            "pass": not totals["missing_metrics"],
            "values": {
                "missing_metric_repeat_count": totals["missing_metric_repeat_count"],
                "missing_metrics": totals["missing_metrics"],
            },
        },
    ]
    failure_point = next((item["name"] for item in criteria if not item["pass"]), None)
    totals["pass_criteria"] = {
        "pass": failure_point is None,
        "failure_point": failure_point,
        "criteria": criteria,
    }
    totals["failure_point"] = failure_point
    totals["pass"] = failure_point is None
    return totals


def repeated_metrics_csv_rows(repeats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in repeats:
        measurement = entry.get("measurement_summary") or {}
        pass_criteria = entry.get("pass_criteria") or measurement.get("pass_criteria") or {}
        throughput = measurement.get("validated_output_throughput") or {}
        latency = measurement.get("capture_to_output_latency_seconds") or {}
        worker_startup = measurement.get("persistent_decode_worker_startup_seconds") or {}
        worker_request = measurement.get("persistent_decode_request_response_seconds") or {}
        worker_processing = measurement.get("persistent_decode_worker_processing_seconds") or {}
        batch_size = measurement.get("persistent_decode_batch_size") or {}
        actual_batch_size = measurement.get("persistent_decode_actual_batch_size") or batch_size
        batch_request = measurement.get("persistent_decode_batch_request_response_seconds") or {}
        batch_processing = measurement.get("persistent_decode_batch_worker_processing_seconds") or {}
        rows.append(
            {
                "repeat_index": entry.get("repeat_index"),
                "run_id": entry.get("run_id"),
                "status": entry.get("status"),
                "pass": entry.get("pass"),
                "failure_point": pass_criteria.get("failure_point"),
                "event_id": measurement.get("event_id"),
                "decode_worker_mode": measurement.get("decode_worker_mode"),
                "output_write_strategy": measurement.get("output_write_strategy"),
                "input_rate_pps": measurement.get("input_rate_pps"),
                "packets_per_repeat": measurement.get("packets_per_repeat"),
                "chunk_packets": measurement.get("chunk_packets"),
                "decoder_workers": measurement.get("decoder_workers"),
                "queue_capacity_chunks": measurement.get("queue_capacity_chunks"),
                "bounded_queues": measurement.get("bounded_queues"),
                "sent_packets": measurement.get("sent_packets"),
                "captured_packets": measurement.get("captured_packets"),
                "decoded_packets": measurement.get("decoded_packets"),
                "decoded_rows": measurement.get("decoded_rows"),
                "validation_pass": measurement.get("validation_pass"),
                "tcpdump_drops": measurement.get("tcpdump_drops"),
                "max_queue_depth_chunks": measurement.get("max_queue_depth_chunks"),
                "final_queue_depth_chunks": measurement.get("final_queue_depth_chunks"),
                "backpressure_blocked_put_count": measurement.get("backpressure_blocked_put_count"),
                "backpressure_blocking_seconds": measurement.get("backpressure_blocking_seconds"),
                "writer_queue_depth_chunks": measurement.get("writer_queue_depth_chunks"),
                "writer_queue_max_depth_after_put_chunks": measurement.get("writer_queue_max_depth_after_put_chunks"),
                "writer_queue_blocked_put_count": measurement.get("writer_queue_blocked_put_count"),
                "writer_queue_blocking_seconds": measurement.get("writer_queue_blocking_seconds"),
                "max_ordered_write_buffer_depth_chunks": (entry.get("queue_metrics") or {}).get("max_ordered_write_buffer_depth_chunks"),
                "capture_time_total_seconds": (measurement.get("capture_time_seconds") or {}).get("total"),
                "decode_time_total_seconds": (measurement.get("decode_time_seconds") or {}).get("total"),
                "persistent_decode_worker_startup_total_seconds": worker_startup.get("total"),
                "persistent_decode_request_response_total_seconds": worker_request.get("total"),
                "persistent_decode_worker_processing_total_seconds": worker_processing.get("total"),
                "requested_batch_size_chunks": measurement.get("requested_batch_size_chunks"),
                "persistent_decode_batch_chunks_configured": measurement.get("persistent_decode_batch_chunks_configured"),
                "decoder_request_count": measurement.get("decoder_request_count"),
                "batch_count": measurement.get("batch_count"),
                "persistent_decode_batch_count": measurement.get("persistent_decode_batch_count"),
                "persistent_decode_multi_chunk_batch_count": measurement.get("persistent_decode_multi_chunk_batch_count"),
                "singleton_batch_count": measurement.get("singleton_batch_count"),
                "partial_batch_count": measurement.get("partial_batch_count"),
                "persistent_decode_batch_size_min": batch_size.get("min"),
                "persistent_decode_batch_size_max": batch_size.get("max"),
                "persistent_decode_batch_size_mean": batch_size.get("mean"),
                "persistent_decode_batch_size_p50": batch_size.get("p50"),
                "persistent_decode_batch_size_p95": batch_size.get("p95"),
                "actual_batch_size_chunks_min": actual_batch_size.get("min"),
                "actual_batch_size_chunks_mean": actual_batch_size.get("mean"),
                "actual_batch_size_chunks_p50": actual_batch_size.get("p50"),
                "actual_batch_size_chunks_p95": actual_batch_size.get("p95"),
                "actual_batch_size_chunks_max": actual_batch_size.get("max"),
                "persistent_decode_actual_batch_size_max": actual_batch_size.get("max"),
                "persistent_decode_actual_batch_size_mean": actual_batch_size.get("mean"),
                "every_chunk_submitted_exactly_once": measurement.get("every_chunk_submitted_exactly_once"),
                "every_result_returned_exactly_once": measurement.get("every_result_returned_exactly_once"),
                "ordered_output_includes_every_chunk_exactly_once": measurement.get("ordered_output_includes_every_chunk_exactly_once"),
                "persistent_decode_batch_request_response_total_seconds": batch_request.get("total"),
                "persistent_decode_batch_worker_processing_total_seconds": batch_processing.get("total"),
                "persistent_decode_worker_failures": measurement.get("persistent_decode_worker_failures"),
                "persistent_decode_fallback_used": measurement.get("persistent_decode_fallback_used"),
                "write_append_time_total_seconds": (measurement.get("write_append_time_seconds") or {}).get("total"),
                "ordered_merge_time_total_seconds": (measurement.get("ordered_merge_time_seconds") or {}).get("total"),
                "ordered_merge_time_max_seconds": (measurement.get("ordered_merge_time_seconds") or {}).get("max"),
                "ordered_merge_request_response_total_seconds": (
                    measurement.get("ordered_merge_request_response_seconds") or {}
                ).get("total"),
                "ordered_merge_count": measurement.get("ordered_merge_count"),
                "validation_time_seconds": measurement.get("validation_time_seconds"),
                "capture_to_output_latency_p95_seconds": latency.get("p95"),
                "capture_to_output_latency_max_seconds": latency.get("max"),
                "total_runtime_seconds": measurement.get("total_runtime_seconds"),
                "validated_output_packets_per_second": throughput.get("packets_per_second"),
                "validated_output_rows_per_second": throughput.get("rows_per_second"),
                "validated_output_keeps_up_with_input_rate": throughput.get("keeps_up_with_input_rate"),
                "required_metric_fields_emitted": measurement.get("required_metric_fields_emitted"),
                "required_metric_gaps": ",".join(measurement.get("required_metric_gaps") or []),
                "measurement_summary_json": entry.get("summary"),
                "manifest": entry.get("manifest"),
            }
        )
    return rows


def write_repeated_summary(series_dir: Path, payload: dict[str, Any], report_mirror_dir: Path | None = None) -> list[str]:
    reports_dir = series_dir / "reports"
    atomic_write_json(reports_dir / "repeated_replay_summary.json", payload)
    rows = repeated_metrics_csv_rows(payload.get("repeats") or [])
    if rows:
        write_csv_rows(reports_dir / "repeated_replay_metrics.csv", rows)
    return mirror_report_files(reports_dir, report_mirror_dir, SERIES_REPORT_FILES)


def run_repeated_replay(args: argparse.Namespace, config_paths: list[Path]) -> dict[str, Any]:
    if args.repeat < 1:
        raise PipelineError("--repeat must be at least 1")
    if args.follow:
        raise PipelineError("--repeat is for bounded pcap_replay; do not combine it with --follow")
    if args.repeat_config and args.repeat < 2:
        raise PipelineError("--repeat-config requires --repeat 2 or higher")

    resolved_paths = [path.expanduser().resolve() for path in config_paths]
    base_config = load_config(resolved_paths[0])
    experiment = base_config.get("experiment") or {}
    expected_repeats = experiment.get("repeats") if isinstance(experiment, dict) else None
    if expected_repeats not in (None, "") and int(expected_repeats) != int(args.repeat):
        raise PipelineError(f"experiment.repeats={expected_repeats} does not match --repeat={args.repeat}")
    series_output_root = output_root_for_config(base_config, args.output_root)
    series_id = args.run_id or f"{stamp()}-repeated-replay"
    series_dir = series_output_root / "runs" / series_id
    (series_dir / "reports").mkdir(parents=True, exist_ok=True)
    report_mirror_root = report_mirror_root_for_config(base_config)
    series_report_mirror_dir = (
        next_available_dir(report_mirror_root / series_id / "reports")
        if report_mirror_root is not None and mirror_series_reports_for_config(base_config)
        else None
    )

    repeats: list[dict[str, Any]] = []
    payload: dict[str, Any] = {
        "summary_type": "iceberg_fabric_repeated_replay_summary",
        "schema_version": 1,
        "status": "dry_run" if args.dry_run else "running",
        "dry_run": bool(args.dry_run),
        "series_id": series_id,
        "series_dir": str(series_dir),
        "output_paths": {
            "series_summary_json": str(series_dir / "reports" / "repeated_replay_summary.json"),
            "series_metrics_csv": str(series_dir / "reports" / "repeated_replay_metrics.csv"),
            "series_report_mirror_dir": str(series_report_mirror_dir) if series_report_mirror_dir else None,
            "per_repeat_summary_pattern": str(series_output_root / "runs" / f"{series_id}-repeatNNN-<label>" / "reports" / "summary.json"),
            "per_repeat_manifest_pattern": str(series_output_root / "runs" / f"{series_id}-repeatNNN-<label>" / "reports" / "manifest.json"),
        },
        "report_mirror_policy": {
            "report_mirror_root": str(report_mirror_root) if report_mirror_root else None,
            "mirror_series_reports": mirror_series_reports_for_config(base_config),
            "mirror_per_repeat_reports": mirror_per_repeat_reports_for_config(base_config),
        },
        "repeat_count": args.repeat,
        "experiment": experiment if isinstance(experiment, dict) else {},
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
    write_repeated_summary(series_dir, payload, series_report_mirror_dir)

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
                pipeline_mode=args.pipeline_mode,
                persistent_batch_chunks=args.persistent_batch_chunks,
                persistent_batch_collect_timeout_seconds=args.persistent_batch_collect_timeout,
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
            write_repeated_summary(series_dir, payload, series_report_mirror_dir)

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
                    "failure_point": result.get("failure_point") if not args.dry_run else None,
                    "pass_criteria": result.get("pass_criteria") or manifest.get("pass_criteria"),
                    "manifest": str(manifest_path),
                    "summary": str(runner.layout.summary_json),
                    "measurement_csv": str(runner.layout.measurement_csv),
                    "queue_metrics_history_csv": str(runner.layout.queue_history_csv),
                    "report_mirror_dir": str(runner.layout.report_mirror_dir) if runner.layout.report_mirror_dir else None,
                    "report_mirror": result.get("report_mirror"),
                    "local_run_dir": str(runner.layout.local_run_dir),
                    "queue_metrics": manifest.get("queue_metrics") or result.get("queue_metrics") or {},
                    "measurement_summary": result.get("measurement_summary") or manifest.get("measurement_summary") or {},
                    "validation": manifest.get("validation_result") or result.get("validation"),
                    "overlap_metrics": manifest.get("overlap_metrics") or result.get("overlap_metrics"),
                    "decode_worker_assignments": manifest.get("decode_worker_assignments") or result.get("decode_worker_assignments") or [],
                    "persistent_decode_batches": manifest.get("persistent_decode_batches") or result.get("persistent_decode_batches") or [],
                    "persistent_decode_result_records": manifest.get("persistent_decode_result_records") or result.get("persistent_decode_result_records") or [],
                    "ordered_output_records": manifest.get("ordered_output_records") or result.get("ordered_output_records") or [],
                    "chunk_delivery_accounting": manifest.get("chunk_delivery_accounting") or result.get("chunk_delivery_accounting") or {},
                    "total_runtime_seconds": result.get("total_runtime_seconds"),
                    "chunk_count": len(chunks),
                    "decoded_rows": decoded_rows,
                    "queue_tracking": tracking,
                }
            )
            payload["status"] = "dry_run" if args.dry_run else "running"
            payload["total_metrics"] = repeated_totals(repeats)
            write_repeated_summary(series_dir, payload, series_report_mirror_dir)
    except Exception as error:
        if repeats:
            repeats[-1]["status"] = "failed"
            repeats[-1]["error"] = str(error)
        payload["status"] = "failed"
        payload["error"] = str(error)
        payload["total_metrics"] = repeated_totals(repeats)
        write_repeated_summary(series_dir, payload, series_report_mirror_dir)
        raise

    payload["status"] = "dry_run" if args.dry_run else "complete"
    payload["total_metrics"] = repeated_totals(repeats)
    write_repeated_summary(series_dir, payload, series_report_mirror_dir)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified DUNE FABRIC live demo runner.")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--source-mode", choices=["pcap_replay", "live_nic"], default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--adaptive", action="store_true", help="Enable experimental adaptive mode for this run.")
    parser.add_argument(
        "--pipeline-mode",
        choices=["staged", "concurrent"],
        default=None,
        help="Override decode.queue.pipeline_mode. Default config behavior is staged; concurrent enables producer-consumer Queue v2.",
    )
    parser.add_argument("--no-validate", action="store_true", help="Disable validation even if the config enables it.")
    parser.add_argument(
        "--max-live-chunks",
        type=int,
        default=None,
        help="Bound live_nic chunks; for pcap_replay smoke tests, cap replay to this many fixed chunks.",
    )
    parser.add_argument("--follow", action="store_true", help="Allow live_nic to run without max_live_chunks until stop_file appears.")
    parser.add_argument(
        "--persistent-batch-chunks",
        type=int,
        choices=[1, 2, 4],
        default=None,
        help="Override decode.queue.persistent_batch_chunks for the single persistent decoder path.",
    )
    parser.add_argument(
        "--persistent-batch-collect-timeout",
        type=float,
        default=None,
        help="Override decode.queue.persistent_batch_collect_timeout_seconds.",
    )
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
            pipeline_mode=args.pipeline_mode,
            persistent_batch_chunks=args.persistent_batch_chunks,
            persistent_batch_collect_timeout_seconds=args.persistent_batch_collect_timeout,
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

