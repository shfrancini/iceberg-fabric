import tempfile
import unittest
from pathlib import Path

from wibviz.delta_server import read_complete_chunk


class DeltaServerTests(unittest.TestCase):
    def test_missing_file_returns_none(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertIsNone(
                read_complete_chunk(Path(directory) / "missing.csv")
            )

    def test_returns_only_complete_lines_and_advances_offset(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "event.csv"
            csv_path.write_bytes(b"a,b\n1,2\n3,4\n5,6")

            first = read_complete_chunk(csv_path, offset=0, max_bytes=9)
            self.assertEqual(first.data, b"a,b\n1,2\n")
            self.assertEqual(first.next_offset, 8)

            second = read_complete_chunk(
                csv_path,
                offset=first.next_offset,
                max_bytes=100,
            )
            self.assertEqual(second.data, b"3,4\n")
            self.assertEqual(second.next_offset, 12)

            with csv_path.open("ab") as output:
                output.write(b"\n")

            third = read_complete_chunk(
                csv_path,
                offset=second.next_offset,
                max_bytes=100,
            )
            self.assertEqual(third.data, b"5,6\n")
            self.assertEqual(third.next_offset, csv_path.stat().st_size)

    def test_offset_resets_when_output_file_is_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "event.csv"
            csv_path.write_bytes(b"a,b\n1,2\n")

            chunk = read_complete_chunk(csv_path, offset=100, max_bytes=100)

            self.assertTrue(chunk.reset)
            self.assertEqual(chunk.data, b"a,b\n1,2\n")
            self.assertEqual(chunk.next_offset, csv_path.stat().st_size)


if __name__ == "__main__":
    unittest.main()

