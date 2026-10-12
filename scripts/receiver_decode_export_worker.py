#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def atomic_touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(now_iso() + "\n", encoding="utf-8")
    tmp.replace(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Persistent receiver-side wibviz decode/export worker.")
    parser.add_argument("--task-dir", required=True, type=Path)
    parser.add_argument("--status-dir", required=True, type=Path)
    parser.add_argument("--stop-file", required=True, type=Path)
    parser.add_argument("--ready-file", required=True, type=Path)
    parser.add_argument("--exit-file", required=True, type=Path)
    parser.add_argument("--worker-id", required=True, type=int)
    parser.add_argument("--mapping", required=True)
    parser.add_argument("--tpc", required=True)
    parser.add_argument("--plane", required=True)
    parser.add_argument("--poll-interval", type=float, default=0.05)
    parser.add_argument("--verbose", action="store_true")
    return parser


def parse_filter_value(value: str) -> int | None:
    if str(value).lower() == "all":
        return None
    return int(value)


def task_sort_key(path: Path) -> tuple[int, str]:
    try:
        marker = path.name.split("chunk_", 1)[1].split(".", 1)[0]
        return int(marker), path.name
    except (IndexError, ValueError):
        return sys.maxsize, path.name


def claim_task(path: Path, worker_id: int) -> Path | None:
    claimed = path.with_name(path.name + f".worker{worker_id}.claimed")
    try:
        path.rename(claimed)
    except FileNotFoundError:
        return None
    return claimed


def export_stats_payload(stats: Any, output_csv: Path) -> dict[str, Any]:
    from wibviz.export import export_summary_line

    payload = asdict(stats)
    payload.update(
        {
            "summary_found": True,
            "summary": export_summary_line(stats, output_csv),
        }
    )
    return payload


def process_task(task_path: Path, context: Any, worker_id: int) -> dict[str, Any]:
    from wibviz.export import export_pcap_with_context, export_summary_line

    task = json.loads(task_path.read_text(encoding="utf-8"))
    chunk_index = int(task["chunk_index"])
    raw_pcap = Path(task["raw_pcap"])
    output_csv = Path(task["output_csv"])
    partial_csv = Path(str(output_csv) + ".partial")
    status_path = Path(task["status_path"])
    decode_log = Path(task["decode_log"])
    started_marker = Path(task["decode_started_marker"])
    completed_marker = Path(task["decode_completed_marker"])
    limit = task.get("limit")
    expected_packets = task.get("expected_packets")
    sort_packets = bool(task.get("sort_packets", True))
    packet_label = str(task.get("packet_label") or raw_pcap)
    batch_id = task.get("batch_id")
    batch_size = task.get("batch_size")
    batch_index_in_batch = task.get("batch_index_in_batch")

    decode_log.parent.mkdir(parents=True, exist_ok=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    status_path.parent.mkdir(parents=True, exist_ok=True)

    started_at = now_iso()
    atomic_touch(started_marker)
    started = time.monotonic()
    status: dict[str, Any] = {
        "schema_version": 1,
        "status": "running",
        "worker_id": worker_id,
        "chunk_index": chunk_index,
        "task_path": str(task_path),
        "raw_pcap": str(raw_pcap),
        "output_csv": str(output_csv),
        "decode_log": str(decode_log),
        "decode_started_at": started_at,
        "expected_packets": expected_packets,
        "decode_worker_mode": "persistent_receiver_process",
        "batch_id": batch_id,
        "batch_size": batch_size,
        "batch_index_in_batch": batch_index_in_batch,
    }

    try:
        stats = export_pcap_with_context(
            context,
            raw_pcap,
            partial_csv,
            limit=limit,
            append=False,
            sort_packets=sort_packets,
            packet_label=packet_label,
        )
        partial_csv.replace(output_csv)
        completed_at = now_iso()
        atomic_touch(completed_marker)
        elapsed = round(time.monotonic() - started, 6)
        stats_payload = export_stats_payload(stats, output_csv)
        status.update(
            {
                "status": "complete",
                "decode_completed_at": completed_at,
                "worker_processing_seconds": elapsed,
                "export_stats": stats_payload,
                "packets_read": stats.packets_read,
                "packets_parsed": stats.packets_parsed,
                "packets_skipped": stats.packets_skipped,
                "rows_written": stats.rows_written,
            }
        )
        decode_log.write_text(
            "Persistent worker decode complete\n"
            + export_summary_line(stats, output_csv)
            + "\n"
            + json.dumps(status, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        atomic_write_json(status_path, status)
        return status
    except BaseException as error:
        completed_at = now_iso()
        atomic_touch(completed_marker)
        status.update(
            {
                "status": "failed",
                "decode_completed_at": completed_at,
                "worker_processing_seconds": round(time.monotonic() - started, 6),
                "error": f"{type(error).__name__}: {error}",
                "traceback": traceback.format_exc(),
            }
        )
        decode_log.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        atomic_write_json(status_path, status)
        raise
    finally:
        try:
            task_path.unlink()
        except FileNotFoundError:
            pass


def run_worker(args: argparse.Namespace) -> int:
    startup_started = time.monotonic()
    args.task_dir.mkdir(parents=True, exist_ok=True)
    args.status_dir.mkdir(parents=True, exist_ok=True)

    try:
        from wibviz.export import ExportContext

        context = ExportContext(
            tpc=parse_filter_value(args.tpc),
            plane=parse_filter_value(args.plane),
            track_offsets=True,
            verbose=args.verbose,
            mapping_path=args.mapping,
            skip_mapping_check=False,
            sort_packets=True,
        )
        ready = {
            "schema_version": 1,
            "status": "ready",
            "worker_id": args.worker_id,
            "pid": os.getpid(),
            "ready_at": now_iso(),
            "startup_elapsed_seconds": round(time.monotonic() - startup_started, 6),
            "task_dir": str(args.task_dir),
            "status_dir": str(args.status_dir),
            "mapping": str(args.mapping),
            "decode_worker_mode": "persistent_receiver_process",
        }
        atomic_write_json(args.ready_file, ready)

        while True:
            task_paths = sorted(args.task_dir.glob("chunk_*.task.json"), key=task_sort_key)
            if not task_paths:
                if args.stop_file.exists():
                    exit_payload = {
                        "schema_version": 1,
                        "status": "stopped",
                        "worker_id": args.worker_id,
                        "pid": os.getpid(),
                        "stopped_at": now_iso(),
                    }
                    atomic_write_json(args.exit_file, exit_payload)
                    return 0
                time.sleep(args.poll_interval)
                continue

            for task_path in task_paths:
                claimed = claim_task(task_path, args.worker_id)
                if claimed is None:
                    continue
                process_task(claimed, context, args.worker_id)
    except BaseException as error:
        payload = {
            "schema_version": 1,
            "status": "failed",
            "worker_id": args.worker_id,
            "pid": os.getpid(),
            "failed_at": now_iso(),
            "error": f"{type(error).__name__}: {error}",
            "traceback": traceback.format_exc(),
        }
        try:
            atomic_write_json(args.exit_file, payload)
        finally:
            print(json.dumps(payload, indent=2, sort_keys=True), file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> int:
    return run_worker(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
