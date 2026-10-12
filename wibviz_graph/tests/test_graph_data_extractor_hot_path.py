#!/usr/bin/env python3
from __future__ import annotations

import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from wibviz.graph_data_extractor import GraphDataExtractor


def decoded_packet() -> dict:
    return {
        "daqH": SimpleNamespace(
            crate_id=1,
            slot_id=2,
            stream_id=3,
            timestamp=1234,
            seq_id=9,
        ),
        "waveforms": [
            {
                "adc0ch0": 111,
                "adc3ch15": 222,
                "not_a_channel": 333,
            }
        ],
    }


def extractor(verbose: bool = False) -> GraphDataExtractor:
    result = GraphDataExtractor(
        track_offsets=False,
        verbose=verbose,
        mapping_path="/tmp/nonexistent-wibviz-channel-map.csv",
    )
    result.channel_map = {
        (1, 2, 3, 0): (100, 2),
    }
    result.wire_map = {
        100: (42, 0),
    }
    return result


class GraphDataExtractorHotPathTests(unittest.TestCase):
    def test_channel_name_to_index_uses_precomputed_map(self) -> None:
        gde = extractor()

        self.assertEqual(gde._calculate_wibframechan("adc0ch0"), 0)
        self.assertEqual(gde._calculate_wibframechan("adc3ch15"), 63)
        self.assertIsNone(gde._calculate_wibframechan("not_a_channel"))

    def test_schema_only_path_does_not_build_csv_rows_or_log_packets(self) -> None:
        gde = extractor(verbose=False)
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            rows, schemas = gde.process_packet_and_return_schema_data(
                decoded_packet(),
                tpc=0,
                plane=2,
                pkt="chunk.pcap",
                include_csv_rows=False,
            )

        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(rows, [])
        self.assertEqual(len(schemas), 1)
        self.assertEqual(schemas[0].wire_id, 42)
        self.assertEqual(schemas[0].adc_value, 111)
        self.assertEqual(schemas[0].packet_file, "chunk.pcap")

    def test_csv_rows_are_available_when_explicitly_requested(self) -> None:
        gde = extractor()

        rows, schemas = gde.process_packet_and_return_schema_data(
            decoded_packet(),
            tpc=0,
            plane=2,
            pkt="chunk.pcap",
            include_csv_rows=True,
        )

        self.assertEqual(len(schemas), 1)
        self.assertEqual(rows, ["9, 1234, 0, 111, 42, 0, 2, 2, chunk.pcap\n"])

    def test_verbose_mode_keeps_packet_logging_available(self) -> None:
        gde = extractor(verbose=True)
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            gde.process_packet_and_return_schema_data(
                decoded_packet(),
                tpc=0,
                plane=2,
                pkt="chunk.pcap",
            )

        self.assertIn("PACKET: 1234, stream: 3, slot: 2", stdout.getvalue())

    def test_reset_for_chunk_preserves_mapping_and_resets_tick_offsets(self) -> None:
        gde = extractor()
        gde.track_offsets = True
        gde.reset_for_chunk()

        _, first = gde.process_packet_and_return_schema_data(
            decoded_packet(),
            tpc=0,
            plane=2,
            pkt="chunk.pcap",
            include_csv_rows=False,
        )
        _, second = gde.process_packet_and_return_schema_data(
            decoded_packet(),
            tpc=0,
            plane=2,
            pkt="chunk.pcap",
            include_csv_rows=False,
        )
        gde.reset_for_chunk()
        _, reset = gde.process_packet_and_return_schema_data(
            decoded_packet(),
            tpc=0,
            plane=2,
            pkt="chunk.pcap",
            include_csv_rows=False,
        )

        self.assertEqual(first[0].tick, 0)
        self.assertEqual(second[0].tick, 64)
        self.assertEqual(reset[0].tick, 0)
        self.assertEqual(reset[0].wire_id, 42)


if __name__ == "__main__":
    unittest.main()
