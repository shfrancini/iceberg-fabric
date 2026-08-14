import csv
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from wibviz.graph_data_extractor import GraphDataExtractor
from wibviz.schema import GraphStruct


class TestExtractor(GraphDataExtractor):
    def build_schema(
        self,
        seq_id,
        timestamp,
        tick,
        adc_value,
        tpc_target,
        plane_target,
        crate,
        slot,
        stream,
        chan,
        packet_file,
    ):
        return GraphStruct(
            seq_id=seq_id,
            timestamp=timestamp,
            tick=tick,
            adc_value=adc_value,
            wire_id=chan,
            tpc=0 if tpc_target is None else tpc_target,
            slot=slot,
            plane=0 if plane_target is None else plane_target,
            packet_file=packet_file,
        )


def parsed_packet(seq_id=1):
    return {
        "daqH": SimpleNamespace(
            seq_id=seq_id,
            timestamp=100,
            crate_id=8,
            slot_id=2,
            stream_id=0,
        ),
        "waveforms": [
            {"adc0ch0": 10, "adc0ch1": 11},
            {"adc0ch0": 12, "adc0ch1": 13},
        ],
    }


class GraphDataExtractorTests(unittest.TestCase):
    def test_loads_portable_mapping_file(self):
        with tempfile.TemporaryDirectory() as directory:
            mapping_path = Path(directory) / "channel-map.csv"
            with mapping_path.open("w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(
                    output,
                    fieldnames=[
                        "crate",
                        "slot",
                        "stream",
                        "channel",
                        "offline_channel",
                        "plane",
                        "wire_id",
                        "tpc",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "crate": 8,
                        "slot": 2,
                        "stream": 0,
                        "channel": 0,
                        "offline_channel": 191,
                        "plane": 0,
                        "wire_id": 191,
                        "tpc": 0,
                    }
                )

            extractor = GraphDataExtractor(
                track_offsets=True,
                mapping_path=mapping_path,
            )

            self.assertEqual(
                extractor._get_offline_channel_and_plane(8, 2, 0, 0),
                (191, 0),
            )
            self.assertEqual(extractor._get_wire_info(191), (191, 0))
            self.assertEqual(len(extractor.channel_map), 1)
            self.assertEqual(len(extractor.wire_map), 1)

    def test_accumulates_schemas_from_every_tick(self):
        extractor = TestExtractor(track_offsets=False)

        rows, schemas = extractor.process_packet_and_return_schema_data(
            parsed_packet(),
            tpc=0,
            plane=0,
            pkt="event.pcap",
        )

        self.assertEqual(len(rows), 4)
        self.assertEqual(len(schemas), 4)
        self.assertEqual([schema.tick for schema in schemas], [0, 0, 1, 1])

    def test_tick_offsets_advance_between_packets(self):
        extractor = TestExtractor(track_offsets=True)

        _, first = extractor.process_packet_and_return_schema_data(
            parsed_packet(seq_id=1), 0, 0, "event.pcap"
        )
        _, second = extractor.process_packet_and_return_schema_data(
            parsed_packet(seq_id=2), 0, 0, "event.pcap"
        )

        self.assertEqual([schema.tick for schema in first], [0, 0, 1, 1])
        self.assertEqual([schema.tick for schema in second], [64, 64, 65, 65])

    def test_tick_offsets_are_not_shared_between_extractors(self):
        first = TestExtractor(track_offsets=True)
        second = TestExtractor(track_offsets=True)

        first.process_packet_and_return_schema_data(
            parsed_packet(), 0, 0, "event.pcap"
        )
        _, schemas = second.process_packet_and_return_schema_data(
            parsed_packet(), 0, 0, "event.pcap"
        )

        self.assertEqual([schema.tick for schema in schemas], [0, 0, 1, 1])


if __name__ == "__main__":
    unittest.main()
