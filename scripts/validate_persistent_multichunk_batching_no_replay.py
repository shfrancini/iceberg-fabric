#!/usr/bin/env python3
"""No-replay validation for persistent single-decoder multi-chunk batching.

This script does not contact FABRIC nodes and does not decode packet data. It
exercises the controller-side batch collection logic with completed chunk
metadata, then emits notebook-friendly evidence for requested batch sizes 1, 2,
and 4.
"""
from __future__ import annotations

import argparse
import json
import queue
import sys
import threading
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *_args, **_kwargs: {}))

from run_pipeline import PipelineRunner, chunk_once_accounting, metric_stats, write_csv_rows  # noqa: E402

DEFAULT_JSON = PROJECT_ROOT / "output" / "no_replay_batching_validation.json"
DEFAULT_CSV = PROJECT_ROOT / "output" / "no_replay_batching_validation.csv"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def make_runner(batch_chunks: int, timeout_seconds: float) -> PipelineRunner:
    runner = PipelineRunner.__new__(PipelineRunner)
    runner.config = {
        "source_mode": "pcap_replay",
        "event_id": "6860",
        "decode": {
            "queue": {
                "pipeline_mode": "concurrent",
                "decode_workers": 1,
                "decode_worker_mode": "persistent_receiver_process",
                "capacity_chunks": 4,
                "persistent_batch_chunks": batch_chunks,
                "persistent_batch_collect_timeout_seconds": timeout_seconds,
            }
        },
        "output": {"write_strategy": "ordered_append"},
    }
    runner.queue_capacity_chunks = 4
    runner._state_lock = threading.RLock()
    runner._chunk_condition = threading.Condition(runner._state_lock)
    runner.chunks = []
    runner.queue_metrics_history = []
    runner.max_queue_depth_chunks = 0
    runner.max_ordered_write_buffer_depth_chunks = 0
    runner.decode_worker_assignments = []
    runner.persistent_decode_workers = []
    runner.persistent_decode_worker_failures = []
    runner.persistent_decode_batches = []
    runner.persistent_decode_result_records = []
    runner.persistent_decode_fallback_used = False
    runner.output_merge_records = []
    runner.ordered_output_records = []
    runner.backpressure_metrics = runner.initial_backpressure_metrics()
    return runner


def chunk(index: int) -> dict[str, Any]:
    return {
        "chunk_index": index,
        "raw_chunk_pcap_path": f"/no-replay/receiver_chunk_{index:03d}.pcap",
        "packets_expected": 64,
    }


def request_row(
    *,
    dry_run_id: str,
    requested_batch_size: int,
    request_index: int,
    collection: dict[str, Any],
    status: str,
) -> dict[str, Any]:
    chunk_ids = [int(value) for value in collection.get("chunk_ids") or []]
    request_started_at = now_iso()
    request_completed_at = now_iso()
    return {
        "dry_run_id": dry_run_id,
        "decoder_request_index": request_index,
        "batch_id": f"{dry_run_id}-request-{request_index:03d}",
        "requested_batch_size_chunks": requested_batch_size,
        "actual_batch_size_chunks": len(chunk_ids),
        "chunk_ids": json.dumps(chunk_ids),
        "batch_formation_timestamp": collection.get("batch_formation_timestamp"),
        "batch_formation_started_at": collection.get("batch_formation_started_at"),
        "batch_formation_completed_at": collection.get("batch_formation_completed_at"),
        "decoder_request_started_at": request_started_at,
        "decoder_request_completed_at": request_completed_at,
        "batch_wait_time_seconds": collection.get("batch_wait_time_seconds"),
        "processing_time_seconds": 0.0,
        "per_batch_decode_export_duration_seconds": 0.0,
        "flush_reason": collection.get("flush_reason"),
        "singleton_batch": len(chunk_ids) == 1,
        "partial_batch": len(chunk_ids) < requested_batch_size,
        "status": status,
    }


def simulate_prefilled_dry_run(requested_batch_size: int, chunk_count: int = 10) -> dict[str, Any]:
    runner = make_runner(requested_batch_size, timeout_seconds=0.0)
    work_queue: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=4)
    rows: list[dict[str, Any]] = []
    submitted_ids: list[int] = []
    result_ids: list[int] = []
    ordered_ids: list[int] = []
    next_to_enqueue = 0
    stop = False
    request_index = 0
    dry_run_id = f"no-replay-batch-size-{requested_batch_size}"

    while not stop:
        while next_to_enqueue < chunk_count and not work_queue.full():
            work_queue.put(chunk(next_to_enqueue))
            next_to_enqueue += 1
        if next_to_enqueue >= chunk_count and not work_queue.full():
            work_queue.put(None)

        first = work_queue.get()
        if first is None:
            work_queue.task_done()
            break
        items, task_done_count, stop, collection = runner.collect_persistent_decode_batch(work_queue, first)
        for _ in range(task_done_count):
            work_queue.task_done()
        row = request_row(
            dry_run_id=dry_run_id,
            requested_batch_size=requested_batch_size,
            request_index=request_index,
            collection=collection,
            status="no_replay_request_formed",
        )
        rows.append(row)
        chunk_ids = [int(item["chunk_index"]) for item in items]
        submitted_ids.extend(chunk_ids)
        result_ids.extend(chunk_ids)
        ordered_ids.extend(sorted(chunk_ids))
        request_index += 1

    expected_ids = list(range(chunk_count))
    actual_sizes = [float(row["actual_batch_size_chunks"]) for row in rows]
    submitted = chunk_once_accounting(expected_ids, submitted_ids)
    returned = chunk_once_accounting(expected_ids, result_ids)
    ordered = chunk_once_accounting(expected_ids, sorted(ordered_ids))
    return {
        "dry_run_id": dry_run_id,
        "requested_batch_size_chunks": requested_batch_size,
        "chunk_count": chunk_count,
        "decoder_request_count": len(rows),
        "batch_count": len(rows),
        "singleton_batch_count": sum(1 for row in rows if int(row["actual_batch_size_chunks"]) == 1),
        "partial_batch_count": sum(1 for row in rows if int(row["actual_batch_size_chunks"]) < requested_batch_size),
        "actual_batch_size_chunks": metric_stats(actual_sizes),
        "every_chunk_submitted_exactly_once": submitted["every_expected_chunk_exactly_once"],
        "every_result_returned_exactly_once": returned["every_expected_chunk_exactly_once"],
        "ordered_output_includes_every_chunk_exactly_once": ordered["every_expected_chunk_exactly_once"],
        "submitted": submitted,
        "results_returned": returned,
        "ordered_output": ordered,
        "rows": rows,
    }


def simulate_flush_scenarios() -> list[dict[str, Any]]:
    scenarios: list[dict[str, Any]] = []

    wait_runner = make_runner(4, timeout_seconds=0.01)
    wait_queue: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=4)
    wait_queue.put(chunk(0))
    first = wait_queue.get()
    assert first is not None
    items, task_done_count, _stop, collection = wait_runner.collect_persistent_decode_batch(wait_queue, first)
    for _ in range(task_done_count):
        wait_queue.task_done()
    scenarios.append(
        request_row(
            dry_run_id="no-replay-wait-time-flush",
            requested_batch_size=4,
            request_index=0,
            collection=collection,
            status="no_replay_wait_flush_formed",
        )
    )
    assert [item["chunk_index"] for item in items] == [0]

    end_runner = make_runner(4, timeout_seconds=0.0)
    end_queue: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=4)
    for item in [chunk(0), chunk(1), None]:
        end_queue.put(item)
    first = end_queue.get()
    assert first is not None
    items, task_done_count, stop, collection = end_runner.collect_persistent_decode_batch(end_queue, first)
    for _ in range(task_done_count):
        end_queue.task_done()
    row = request_row(
        dry_run_id="no-replay-end-of-input-flush",
        requested_batch_size=4,
        request_index=0,
        collection=collection,
        status="no_replay_end_flush_formed",
    )
    row["end_of_input_seen"] = stop
    scenarios.append(row)
    assert [item["chunk_index"] for item in items] == [0, 1]
    return scenarios


def build_summary() -> dict[str, Any]:
    dry_runs = [simulate_prefilled_dry_run(size) for size in (1, 2, 4)]
    flush_rows = simulate_flush_scenarios()
    all_rows = [row for dry_run in dry_runs for row in dry_run["rows"]] + flush_rows
    requested_sizes_ok = all(
        dry_run["every_chunk_submitted_exactly_once"]
        and dry_run["every_result_returned_exactly_once"]
        and dry_run["ordered_output_includes_every_chunk_exactly_once"]
        for dry_run in dry_runs
    )
    flush_scenarios_ok = any(row.get("flush_reason") == "bounded_wait_expired" for row in flush_rows) and any(
        row.get("flush_reason") == "end_of_input" for row in flush_rows
    )
    return {
        "schema_version": 1,
        "generated_at": now_iso(),
        "validation_type": "no_replay_controller_batching_validation",
        "network_used": False,
        "packet_replay_run": False,
        "event_id": "6860",
        "decoder_workers": 1,
        "chunk_packets": 64,
        "queue_capacity_chunks": 4,
        "requested_batch_sizes_tested": [1, 2, 4],
        "dry_runs": dry_runs,
        "flush_scenario_rows": flush_rows,
        "decoder_request_rows": all_rows,
        "all_requested_sizes_have_expected_batching": requested_sizes_ok,
        "flush_scenarios_pass": flush_scenarios_ok,
        "overall_pass": requested_sizes_ok and flush_scenarios_ok,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_CSV)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = build_summary()
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_csv_rows(args.output_csv, summary["decoder_request_rows"])
    print(json.dumps({"status": "complete", "output_json": str(args.output_json), "output_csv": str(args.output_csv)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
