#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import queue
import subprocess
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ROOT.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *_args, **_kwargs: {}))

from receiver_decode_export_worker import process_task
from run_pipeline import PipelineRunner
from wibviz.export import ExportContext, export_pcap


PCAP = PROJECT_ROOT / "pcap" / "icecosmich_run82134054_sub001_evt006860.pcap"
MAPPING = ROOT / "event-4245-channel-map.csv"


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


class PersistentWorkerExportTests(unittest.TestCase):
    def require_export_fixtures(self) -> None:
        if not PCAP.is_file():
            self.skipTest(f"missing PCAP fixture: {PCAP}")
        if not MAPPING.is_file():
            self.skipTest(f"missing mapping fixture: {MAPPING}")

    def task_payload(
        self,
        tmpdir: Path,
        index: int,
        output_csv: Path,
        *,
        batch_id: str | None = None,
        batch_size: int | None = None,
        batch_index_in_batch: int | None = None,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": 1,
            "command": "decode_chunk",
            "decode_worker_mode": "persistent_receiver_process",
            "worker_id": 1,
            "chunk_index": index,
            "raw_pcap": str(PCAP),
            "output_csv": str(output_csv),
            "status_path": str(tmpdir / f"chunk_{index:03d}.status.json"),
            "decode_log": str(tmpdir / f"chunk_{index:03d}.log"),
            "decode_started_marker": str(tmpdir / f"chunk_{index:03d}_started.txt"),
            "decode_completed_marker": str(tmpdir / f"chunk_{index:03d}_completed.txt"),
            "expected_packets": 4,
            "limit": 4,
            "sort_packets": True,
            "packet_label": str(PCAP),
        }
        if batch_id is not None:
            payload["batch_id"] = batch_id
            payload["batch_size"] = batch_size
            payload["batch_index_in_batch"] = batch_index_in_batch
        return payload

    def test_persistent_worker_context_matches_reference_export_and_resets(self) -> None:
        self.require_export_fixtures()
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            reference_csv = tmpdir / "reference.csv"
            first_worker_csv = tmpdir / "worker_first.csv"
            second_worker_csv = tmpdir / "worker_second.csv"

            reference_stats = export_pcap(
                PCAP,
                reference_csv,
                tpc=0,
                plane=2,
                limit=4,
                mapping_path=MAPPING,
            )
            context = ExportContext(
                tpc=0,
                plane=2,
                mapping_path=MAPPING,
            )

            task_one_path = tmpdir / "chunk_000.task.json"
            task_one_path.write_text(json.dumps(self.task_payload(tmpdir, 0, first_worker_csv)), encoding="utf-8")
            first_status = process_task(task_one_path, context, worker_id=1)

            task_two_path = tmpdir / "chunk_001.task.json"
            task_two_path.write_text(json.dumps(self.task_payload(tmpdir, 1, second_worker_csv)), encoding="utf-8")
            second_status = process_task(task_two_path, context, worker_id=1)

            for status in (first_status, second_status):
                self.assertEqual(status["status"], "complete")
                self.assertEqual(status["packets_read"], reference_stats.packets_read)
                self.assertEqual(status["packets_parsed"], reference_stats.packets_parsed)
                self.assertEqual(status["packets_skipped"], reference_stats.packets_skipped)
                self.assertEqual(status["rows_written"], reference_stats.rows_written)
                self.assertGreater(status["rows_written"], 0)

            self.assertEqual(reference_csv.read_text(encoding="utf-8"), first_worker_csv.read_text(encoding="utf-8"))
            self.assertEqual(reference_csv.read_text(encoding="utf-8"), second_worker_csv.read_text(encoding="utf-8"))
            self.assertEqual(read_rows(reference_csv), read_rows(first_worker_csv))
            self.assertEqual(read_rows(reference_csv), read_rows(second_worker_csv))

            self.assertFalse(task_one_path.exists())
            self.assertFalse(task_two_path.exists())

    def test_ordered_chunk_merge_preserves_header_and_chunk_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            first = tmpdir / "chunk_000.csv"
            second = tmpdir / "chunk_001.csv"
            output = tmpdir / "combined.csv"
            first.write_text("a,b\n0,first\n1,first\n", encoding="utf-8")
            second.write_text("a,b\n2,second\n3,second\n", encoding="utf-8")

            runner = PipelineRunner.__new__(PipelineRunner)
            runner.config = {
                "slice": {"receiver_node": "local"},
                "decode": {"queue": {"pipeline_mode": "concurrent", "decode_workers": 1}},
                "output": {"write_strategy": "ordered_chunk_merge"},
            }
            runner.layout = SimpleNamespace(remote_combined_csv=str(output))
            runner._state_lock = threading.RLock()
            runner._chunk_condition = threading.Condition(runner._state_lock)
            runner.output_merge_records = []
            runner.write_manifest = lambda **_kwargs: None

            def local_exec(_node: str, command: str, timeout: int = 60, check: bool = False):
                return subprocess.run(
                    command,
                    shell=True,
                    check=False,
                    text=True,
                    capture_output=True,
                    timeout=timeout,
                )

            runner.remote_exec = local_exec
            chunks = [
                {
                    "chunk_index": 1,
                    "decoded_csv_path": str(second),
                    "decode_export_statistics": {"rows_written": 2},
                    "_capture_end_monotonic": 1.0,
                },
                {
                    "chunk_index": 0,
                    "decoded_csv_path": str(first),
                    "decode_export_statistics": {"rows_written": 2},
                    "_capture_end_monotonic": 1.0,
                },
            ]

            rows = runner.merge_decoded_chunks_ordered(chunks)

            self.assertEqual(rows, 4)
            self.assertEqual(output.read_text(encoding="utf-8"), "a,b\n0,first\n1,first\n2,second\n3,second\n")
            self.assertEqual([chunk["append_status"] for chunk in chunks], ["appended", "appended"])
            ordered_chunks = sorted(chunks, key=lambda item: item["chunk_index"])
            self.assertEqual([chunk["cumulative_rows"] for chunk in ordered_chunks], [2, 4])
            self.assertEqual(runner.output_merge_records[0]["chunk_indices"], [0, 1])
            self.assertEqual(runner.output_merge_records[0]["combined_rows"], 4)

    def test_ordered_chunk_merge_rejects_non_contiguous_chunks(self) -> None:
        runner = PipelineRunner.__new__(PipelineRunner)
        with self.assertRaisesRegex(Exception, "non-contiguous"):
            runner.merge_decoded_chunks_ordered(
                [
                    {"chunk_index": 0, "decoded_csv_path": "chunk_000.csv"},
                    {"chunk_index": 2, "decoded_csv_path": "chunk_002.csv"},
                ]
            )

    def test_scheduler_collects_already_ready_chunks_into_one_batch(self) -> None:
        runner = PipelineRunner.__new__(PipelineRunner)
        runner.config = {
            "decode": {
                "queue": {
                    "decode_worker_mode": "persistent_receiver_process",
                    "persistent_batch_chunks": 4,
                    "persistent_batch_collect_timeout_seconds": 0.0,
                }
            }
        }
        runner.queue_capacity_chunks = 4
        runner._state_lock = threading.RLock()
        runner.backpressure_metrics = runner.initial_backpressure_metrics()

        work_queue: queue.Queue[dict[str, object] | None] = queue.Queue(maxsize=4)
        first = {"chunk_index": 0}
        work_queue.put({"chunk_index": 1})
        work_queue.put({"chunk_index": 2})

        items, task_done_count, stop_after_batch, collection = runner.collect_persistent_decode_batch(work_queue, first)

        self.assertFalse(stop_after_batch)
        self.assertEqual(task_done_count, 3)
        self.assertEqual([item["chunk_index"] for item in items], [0, 1, 2])
        self.assertEqual(collection["configured_batch_limit"], 4)
        self.assertEqual(collection["queue_depth_after_first_get"], 2)
        self.assertEqual(collection["queue_ready_drained_count"], 2)
        self.assertEqual(collection["queue_depth_after_drain"], 0)
        self.assertEqual(collection["actual_batch_size"], 3)
        self.assertTrue(collection["multi_chunk_batch"])
        self.assertEqual(runner.backpressure_metrics["raw_to_decode"]["get_count"], 3)

    def test_batched_persistent_worker_tasks_match_reference_export(self) -> None:
        self.require_export_fixtures()
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            reference_csv = tmpdir / "reference.csv"
            first_worker_csv = tmpdir / "batched_worker_first.csv"
            second_worker_csv = tmpdir / "batched_worker_second.csv"
            batch_id = "unit-batch-000-001"

            reference_stats = export_pcap(
                PCAP,
                reference_csv,
                tpc=0,
                plane=2,
                limit=4,
                mapping_path=MAPPING,
            )
            context = ExportContext(
                tpc=0,
                plane=2,
                mapping_path=MAPPING,
            )

            statuses = []
            for index, output_csv in enumerate((first_worker_csv, second_worker_csv)):
                task_path = tmpdir / f"chunk_{index:03d}.task.json"
                task_path.write_text(
                    json.dumps(
                        self.task_payload(
                            tmpdir,
                            index,
                            output_csv,
                            batch_id=batch_id,
                            batch_size=2,
                            batch_index_in_batch=index,
                        )
                    ),
                    encoding="utf-8",
                )
                statuses.append(process_task(task_path, context, worker_id=1))

            for index, status in enumerate(statuses):
                self.assertEqual(status["status"], "complete")
                self.assertEqual(status["batch_id"], batch_id)
                self.assertEqual(status["batch_size"], 2)
                self.assertEqual(status["batch_index_in_batch"], index)
                self.assertEqual(status["packets_read"], reference_stats.packets_read)
                self.assertEqual(status["packets_parsed"], reference_stats.packets_parsed)
                self.assertEqual(status["packets_skipped"], reference_stats.packets_skipped)
                self.assertEqual(status["rows_written"], reference_stats.rows_written)

            self.assertEqual(reference_csv.read_text(encoding="utf-8"), first_worker_csv.read_text(encoding="utf-8"))
            self.assertEqual(reference_csv.read_text(encoding="utf-8"), second_worker_csv.read_text(encoding="utf-8"))
            self.assertEqual(read_rows(reference_csv), read_rows(first_worker_csv))
            self.assertEqual(read_rows(reference_csv), read_rows(second_worker_csv))


if __name__ == "__main__":
    unittest.main()
