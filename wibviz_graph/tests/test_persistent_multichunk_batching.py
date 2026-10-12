#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import queue
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ROOT.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *_args, **_kwargs: {}))

from receiver_decode_export_worker import process_task
from run_pipeline import PipelineRunner, chunk_once_accounting
from wibviz.export import ExportContext


PCAP = PROJECT_ROOT / "pcap" / "icecosmich_run82134054_sub001_evt006860.pcap"
MAPPING = ROOT / "event-4245-channel-map.csv"


def make_runner(batch_chunks: int, timeout_seconds: float = 0.0) -> PipelineRunner:
    runner = PipelineRunner.__new__(PipelineRunner)
    runner.config = {
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


def chunk(index: int) -> dict[str, int]:
    return {"chunk_index": index}


def collect_from_queue(
    runner: PipelineRunner,
    queued: list[dict[str, int] | None],
) -> tuple[list[dict[str, int]], int, bool, dict[str, object], queue.Queue[dict[str, int] | None]]:
    work_queue: queue.Queue[dict[str, int] | None] = queue.Queue(maxsize=4)
    for item in queued:
        work_queue.put(item)
    first = work_queue.get()
    assert first is not None
    items, task_done_count, stop_after_batch, collection = runner.collect_persistent_decode_batch(work_queue, first)
    for _ in range(task_done_count):
        work_queue.task_done()
    return items, task_done_count, stop_after_batch, collection, work_queue


def task_payload(tmpdir: Path, index: int, output_csv: Path, batch_id: str, batch_size: int, batch_index: int) -> dict[str, object]:
    return {
        "schema_version": 1,
        "command": "decode_chunk",
        "decode_worker_mode": "persistent_receiver_process",
        "worker_id": 1,
        "chunk_index": index,
        "batch_id": batch_id,
        "batch_size": batch_size,
        "batch_index_in_batch": batch_index,
        "raw_pcap": str(PCAP),
        "output_csv": str(output_csv),
        "status_path": str(tmpdir / f"chunk_{index:03d}.status.json"),
        "decode_log": str(tmpdir / f"chunk_{index:03d}.log"),
        "decode_started_marker": str(tmpdir / f"chunk_{index:03d}_started.txt"),
        "decode_completed_marker": str(tmpdir / f"chunk_{index:03d}_completed.txt"),
        "expected_packets": 1,
        "limit": 1,
        "sort_packets": True,
        "packet_label": str(PCAP),
    }


def combined_rows(csv_paths: list[Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in csv_paths:
        with path.open(newline="", encoding="utf-8") as handle:
            rows.extend(csv.DictReader(handle))
    return rows


class PersistentMultichunkBatchingTests(unittest.TestCase):
    def test_max_live_chunks_updates_replay_metadata_for_bounded_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = str(Path(tmp) / "runs")
            config = {
                "label": "bounded-smoke",
                "source_mode": "pcap_replay",
                "experiment": {
                    "event_input": "/tmp/input.pcap",
                    "packets_per_repeat": 640,
                    "input_rate_pps": 100,
                    "chunk_packets": 64,
                    "decoder_workers": 1,
                    "queue_capacity_chunks": 4,
                    "bounded_queues": True,
                    "output_root": output_root,
                    "validation_reference_csv": "/tmp/reference.csv",
                },
                "transport": {"type": "loom"},
                "slice": {"name": "slice", "sender_node": "sender", "receiver_node": "receiver"},
                "network": {"sender_interface": "enp7s0", "receiver_interface": "enp7s0"},
                "paths": {
                    "output_root": output_root,
                    "receiver_root": "/home/ubuntu",
                    "sender_root": "/home/ubuntu",
                    "wibviz_src": "/home/ubuntu/wibviz_graph/src",
                    "mapping_csv": "/tmp/mapping.csv",
                },
                "pcap_replay": {"sender_pcap": "/tmp/input.pcap", "pps": 100, "packet_limit": 640},
                "decode": {
                    "chunking": {"mode": "fixed_packet_count", "chunk_packets": 64},
                    "queue": {
                        "pipeline_mode": "concurrent",
                        "decode_workers": 1,
                        "decode_worker_mode": "persistent_receiver_process",
                        "capacity_chunks": 4,
                        "persistent_batch_chunks": 1,
                        "require_bounded_queues": True,
                    },
                },
                "output": {"write_strategy": "ordered_append"},
                "validation": {"enabled": True, "reference_csv": "/tmp/reference.csv", "expected_packets": 640, "expected_rows": 491520},
            }
            runner = PipelineRunner(config, config_path=Path(tmp) / "config.yaml", max_live_chunks=2)
            runner.validate_config()
            payload = runner.dry_run_payload(emit_json=False)

            self.assertEqual(config["pcap_replay"]["packet_limit"], 128)
            self.assertEqual(config["pcap_replay"]["max_replay_chunks"], 2)
            self.assertEqual(config["experiment"]["packets_per_repeat"], 128)
            self.assertEqual(config["experiment"]["full_packets_per_repeat"], 640)
            self.assertEqual(payload["configuration"]["pcap_replay"]["packet_limit"], 128)
            self.assertTrue(payload["dry_run"])

            finite_burst_measurement = {
                "sent_packets": 128,
                "captured_packets": 128,
                "decoded_packets": 128,
                "validation_pass": True,
                "tcpdump_drops": 0,
                "final_queue_depth_chunks": 0,
                "append_backlog_chunks": 0,
                "bounded_queues": True,
                "queue_capacity_chunks": 4,
                "capture_to_output_latency_seconds": {"count": 2, "max": 1.0},
                "validated_output_throughput": {
                    "packets_per_second": 2.0,
                    "rows_per_second": 3072.0,
                    "input_rate_pps": 100,
                    "keeps_up_with_input_rate": False,
                },
                "missing_metrics": [],
                "decoder_workers": 1,
            }
            pass_criteria = runner.build_pass_criteria(finite_burst_measurement)
            self.assertTrue(pass_criteria["pass"])
            self.assertIsNone(pass_criteria["failure_point"])
            self.assertIn("recorded separately", pass_criteria["criteria"][7]["name"])

    def require_export_fixtures(self) -> None:
        if not PCAP.is_file():
            self.skipTest(f"missing PCAP fixture: {PCAP}")
        if not MAPPING.is_file():
            self.skipTest(f"missing mapping fixture: {MAPPING}")

    def test_batch_size_1_preserves_singleton_behavior(self) -> None:
        runner = make_runner(batch_chunks=1, timeout_seconds=0.01)
        items, task_done_count, stop, collection, work_queue = collect_from_queue(
            runner,
            [chunk(0), chunk(1), chunk(2)],
        )

        self.assertEqual([item["chunk_index"] for item in items], [0])
        self.assertEqual(task_done_count, 1)
        self.assertFalse(stop)
        self.assertEqual(work_queue.qsize(), 2)
        self.assertEqual(collection["requested_batch_size_chunks"], 1)
        self.assertEqual(collection["actual_batch_size_chunks"], 1)
        self.assertTrue(collection["singleton_batch"])
        self.assertFalse(collection["partial_batch"])

    def test_full_batches_of_size_2(self) -> None:
        runner = make_runner(batch_chunks=2, timeout_seconds=0.0)
        items, _task_done_count, stop, collection, _work_queue = collect_from_queue(
            runner,
            [chunk(0), chunk(1)],
        )

        self.assertFalse(stop)
        self.assertEqual([item["chunk_index"] for item in items], [0, 1])
        self.assertEqual(collection["requested_batch_size_chunks"], 2)
        self.assertEqual(collection["actual_batch_size_chunks"], 2)
        self.assertEqual(collection["flush_reason"], "requested_batch_size_reached")
        self.assertFalse(collection["partial_batch"])

    def test_full_batches_of_size_4(self) -> None:
        runner = make_runner(batch_chunks=4, timeout_seconds=0.0)
        items, _task_done_count, stop, collection, _work_queue = collect_from_queue(
            runner,
            [chunk(0), chunk(1), chunk(2), chunk(3)],
        )

        self.assertFalse(stop)
        self.assertEqual([item["chunk_index"] for item in items], [0, 1, 2, 3])
        self.assertEqual(collection["requested_batch_size_chunks"], 4)
        self.assertEqual(collection["actual_batch_size_chunks"], 4)
        self.assertTrue(collection["multi_chunk_batch"])
        self.assertEqual(collection["flush_reason"], "requested_batch_size_reached")

    def test_final_partial_batch_flushes_on_end_marker(self) -> None:
        runner = make_runner(batch_chunks=4, timeout_seconds=0.0)
        items, task_done_count, stop, collection, _work_queue = collect_from_queue(
            runner,
            [chunk(8), chunk(9), None],
        )

        self.assertTrue(stop)
        self.assertEqual(task_done_count, 3)
        self.assertEqual([item["chunk_index"] for item in items], [8, 9])
        self.assertEqual(collection["actual_batch_size_chunks"], 2)
        self.assertTrue(collection["partial_batch"])
        self.assertEqual(collection["flush_reason"], "end_of_input")

    def test_wait_time_flushes_partial_batch(self) -> None:
        runner = make_runner(batch_chunks=4, timeout_seconds=0.01)
        items, _task_done_count, stop, collection, _work_queue = collect_from_queue(runner, [chunk(0)])

        self.assertFalse(stop)
        self.assertEqual([item["chunk_index"] for item in items], [0])
        self.assertEqual(collection["actual_batch_size_chunks"], 1)
        self.assertTrue(collection["partial_batch"])
        self.assertEqual(collection["flush_reason"], "bounded_wait_expired")
        self.assertGreaterEqual(float(collection["batch_wait_time_seconds"]), 0.0)

    def test_end_of_input_flushes_singleton_batch(self) -> None:
        runner = make_runner(batch_chunks=4, timeout_seconds=0.0)
        items, _task_done_count, stop, collection, _work_queue = collect_from_queue(runner, [chunk(0), None])

        self.assertTrue(stop)
        self.assertEqual([item["chunk_index"] for item in items], [0])
        self.assertEqual(collection["actual_batch_size_chunks"], 1)
        self.assertTrue(collection["partial_batch"])
        self.assertEqual(collection["flush_reason"], "end_of_input")

    def test_no_duplicate_or_missing_chunk_ids_accounting(self) -> None:
        good = chunk_once_accounting([0, 1, 2, 3], [0, 1, 2, 3])
        self.assertTrue(good["every_expected_chunk_exactly_once"])
        self.assertEqual(good["missing_chunk_ids"], [])
        self.assertEqual(good["duplicate_chunk_ids"], [])

        bad = chunk_once_accounting([0, 1, 2, 3], [0, 1, 1, 3])
        self.assertFalse(bad["every_expected_chunk_exactly_once"])
        self.assertEqual(bad["missing_chunk_ids"], [2])
        self.assertEqual(bad["duplicate_chunk_ids"], [1])

    def test_out_of_order_decoder_completion_still_appends_in_order(self) -> None:
        runner = make_runner(batch_chunks=4, timeout_seconds=0.0)
        runner.chunks = [{"chunk_index": index} for index in range(4)]
        decoded_queue: queue.Queue[dict[str, int] | None] = queue.Queue(maxsize=4)
        producer_done = threading.Event()
        errors: list[BaseException] = []
        appended: list[int] = []

        def append_decoded_chunk(item: dict[str, int], cumulative_rows_before: int) -> int:
            index = int(item["chunk_index"])
            appended.append(index)
            runner.ordered_output_records.append(
                {
                    "chunk_index": index,
                    "ordered_output_position": len(runner.ordered_output_records),
                    "status": "appended",
                }
            )
            runner.chunks[index]["append_status"] = "appended"
            return cumulative_rows_before + 1

        runner.append_decoded_chunk = append_decoded_chunk  # type: ignore[method-assign]
        runner.write_manifest = lambda **_kwargs: None  # type: ignore[method-assign]

        thread = threading.Thread(
            target=runner.concurrent_ordered_append_coordinator,
            args=(producer_done, decoded_queue, errors),
        )
        thread.start()
        for index in [2, 0, 3, 1]:
            decoded_queue.put({"chunk_index": index})
        producer_done.set()
        thread.join(timeout=2.0)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(appended, [0, 1, 2, 3])
        accounting = runner.chunk_delivery_accounting()
        self.assertTrue(accounting["ordered_output_includes_every_chunk_exactly_once"])

    def test_decoded_output_equality_for_batch_sizes_1_2_and_4(self) -> None:
        self.require_export_fixtures()
        outputs_by_batch_size: dict[int, list[dict[str, str]]] = {}
        statuses_by_batch_size: dict[int, list[dict[str, object]]] = {}

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            for requested_batch_size in [1, 2, 4]:
                context = ExportContext(tpc=0, plane=2, mapping_path=MAPPING)
                output_paths: list[Path] = []
                statuses: list[dict[str, object]] = []
                for batch_start in range(0, 4, requested_batch_size):
                    batch_indices = list(range(batch_start, min(4, batch_start + requested_batch_size)))
                    batch_id = f"unit-batch-size-{requested_batch_size}-{batch_start:03d}"
                    actual_batch_size = len(batch_indices)
                    for batch_index, chunk_index in enumerate(batch_indices):
                        output_csv = tmpdir / f"batch{requested_batch_size}_chunk_{chunk_index:03d}.csv"
                        task_path = tmpdir / f"batch{requested_batch_size}_chunk_{chunk_index:03d}.task.json"
                        task_path.write_text(
                            json.dumps(
                                task_payload(
                                    tmpdir,
                                    chunk_index,
                                    output_csv,
                                    batch_id,
                                    actual_batch_size,
                                    batch_index,
                                )
                            ),
                            encoding="utf-8",
                        )
                        statuses.append(process_task(task_path, context, worker_id=1))
                        output_paths.append(output_csv)
                outputs_by_batch_size[requested_batch_size] = combined_rows(output_paths)
                statuses_by_batch_size[requested_batch_size] = statuses

        self.assertEqual(outputs_by_batch_size[1], outputs_by_batch_size[2])
        self.assertEqual(outputs_by_batch_size[1], outputs_by_batch_size[4])
        for requested_batch_size, statuses in statuses_by_batch_size.items():
            self.assertEqual([int(status["chunk_index"]) for status in statuses], [0, 1, 2, 3])
            self.assertTrue(all(status["status"] == "complete" for status in statuses))
            self.assertTrue(all(int(status["batch_size"]) <= requested_batch_size for status in statuses))


if __name__ == "__main__":
    unittest.main()
