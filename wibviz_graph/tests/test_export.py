import csv
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from wibviz.export import (
    CSV_FIELDS,
    build_parser,
    check_common_export_inputs,
    export_packets,
    parse_filter,
    write_schema_rows,
)


class FakeProcessor:
    def __init__(self):
        self.calls = 0

    def process_packet_all_ticks(self, packet):
        self.calls += 1
        if packet == "skip":
            return None
        return {"packet": packet}


class FakeExtractor:
    def __init__(self):
        self.calls = []

    def process_packet_and_return_schema_data(self, parsed, tpc, plane, pkt):
        self.calls.append((parsed, tpc, plane, pkt))
        schema = SimpleNamespace(
            seq_id=len(self.calls),
            timestamp=100,
            tick=2,
            adc_value=1234,
            wire_id=8,
            tpc=tpc,
            slot=3,
            plane=plane,
            packet_file=pkt,
        )
        return [], [schema]


class ExportTests(unittest.TestCase):
    def test_parser_accepts_live_interface_source(self):
        args = build_parser().parse_args(
            [
                "--interface",
                "enp7s0",
                "--output",
                "capture.csv",
                "--mapping",
                "channel-map.csv",
            ]
        )

        self.assertEqual(args.interface, "enp7s0")
        self.assertIsNone(args.pcap)
        self.assertEqual(args.output, "capture.csv")

    def test_mapping_file_satisfies_environment_check(self):
        with tempfile.TemporaryDirectory() as directory:
            mapping_path = Path(directory) / "channel-map.csv"
            mapping_path.touch()

            check_common_export_inputs(mapping_path=mapping_path)

    def test_parse_filter_accepts_integer_and_all(self):
        self.assertEqual(parse_filter("2"), 2)
        self.assertIsNone(parse_filter("all"))
        self.assertIsNone(parse_filter("ALL"))

    def test_write_schema_rows_uses_declared_column_order(self):
        output = io.StringIO()
        writer = csv.writer(output)
        schema = SimpleNamespace(
            seq_id=1,
            timestamp=2,
            tick=3,
            adc_value=4,
            wire_id=5,
            tpc=6,
            slot=7,
            plane=8,
            packet_file="event.pcap",
        )

        count = write_schema_rows(writer, [schema])

        self.assertEqual(count, 1)
        self.assertEqual(
            next(csv.reader(io.StringIO(output.getvalue()))),
            ["1", "2", "3", "4", "5", "6", "7", "8", "event.pcap"],
        )

    def test_export_packets_tracks_counts_and_skips_unparsed_packets(self):
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(CSV_FIELDS)
        processor = FakeProcessor()
        extractor = FakeExtractor()

        stats = export_packets(
            ["first", "skip", "third"],
            processor,
            extractor,
            writer,
            packet_label="source.pcap",
            tpc=0,
            plane=2,
        )

        self.assertEqual(stats.packets_read, 3)
        self.assertEqual(stats.packets_parsed, 2)
        self.assertEqual(stats.rows_written, 2)
        self.assertEqual(len(extractor.calls), 2)
        self.assertEqual(extractor.calls[0][1:], (0, 2, "source.pcap"))

    def test_export_packets_honors_limit(self):
        output = io.StringIO()
        processor = FakeProcessor()
        extractor = FakeExtractor()

        stats = export_packets(
            ["first", "second", "third"],
            processor,
            extractor,
            csv.writer(output),
            packet_label="source.pcap",
            tpc=0,
            plane=2,
            limit=1,
        )

        self.assertEqual(stats.packets_read, 1)
        self.assertEqual(processor.calls, 1)


if __name__ == "__main__":
    unittest.main()
