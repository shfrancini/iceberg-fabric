#!/usr/bin/env python3
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from wibviz.export import decoded_packet_sort_key, sort_decoded_packet_records
from wibviz.tick_offset import TickOffsetHelper


def decoded_packet(seq_id: int, timestamp: int | None = None, slot_id: int = 3, stream_id: int = 1) -> dict:
    if timestamp is None:
        timestamp = seq_id * 2048
    return {
        "daqH": SimpleNamespace(
            slot_id=slot_id,
            stream_id=stream_id,
            timestamp=timestamp,
            seq_id=seq_id,
        ),
        "waveforms": [],
    }


class ExportPacketOrderingTests(unittest.TestCase):
    def test_sort_key_uses_detector_metadata_before_original_index(self) -> None:
        packet = decoded_packet(seq_id=21, timestamp=43008, slot_id=3, stream_id=1)
        self.assertEqual(
            decoded_packet_sort_key(packet, original_index=50),
            (3, 1, 43008, 21, 50),
        )

    def test_sort_is_deterministic_and_does_not_drop_packets(self) -> None:
        first = decoded_packet(seq_id=7, timestamp=14336)
        second = decoded_packet(seq_id=7, timestamp=14336)
        third = decoded_packet(seq_id=6, timestamp=12288)
        records = [(2, first), (1, second), (3, third)]

        sorted_records = sort_decoded_packet_records(records)

        self.assertCountEqual([id(packet) for _, packet in sorted_records], [id(first), id(second), id(third)])
        self.assertEqual([index for index, _ in sorted_records], [3, 1, 2])

    def test_out_of_order_stream_packets_are_sorted_before_tick_offsets(self) -> None:
        captured_order = list(range(18)) + [21, 19, 20, 18, 23, 22] + list(range(24, 32))
        records = [
            (capture_index, decoded_packet(seq_id=seq_id))
            for capture_index, seq_id in enumerate(captured_order)
        ]

        helper = TickOffsetHelper(on=True)
        assigned_offsets: dict[int, int] = {}
        for _capture_index, packet in sort_decoded_packet_records(records):
            daq = packet["daqH"]
            tick_offset = helper.get_tick_offset_for_slot_stream(daq.slot_id, daq.stream_id)
            assigned_offsets[daq.seq_id] = tick_offset.offset
            helper.increment_tick_offset_64_for_slot_stream(daq.slot_id, daq.stream_id)

        self.assertEqual([seq for _, packet in sort_decoded_packet_records(records) if (seq := packet["daqH"].seq_id) in {18, 21, 22, 23}], [18, 21, 22, 23])
        self.assertEqual(assigned_offsets[18], 18 * 64)
        self.assertEqual(assigned_offsets[21], 21 * 64)
        self.assertEqual(assigned_offsets[22], 22 * 64)
        self.assertEqual(assigned_offsets[23], 23 * 64)

    def test_tick_offset_helper_state_is_instance_local(self) -> None:
        first = TickOffsetHelper(on=True)
        second = TickOffsetHelper(on=True)
        first.increment_tick_offset_64_for_slot_stream(3, 1)
        self.assertEqual(first.get_tick_offset_for_slot_stream(3, 1).offset, 64)
        self.assertEqual(second.get_tick_offset_for_slot_stream(3, 1).offset, 0)


if __name__ == "__main__":
    unittest.main()
