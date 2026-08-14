import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path

from wibviz.dashboard import load_snapshot


DASHBOARD_DEPENDENCIES_AVAILABLE = all(
    importlib.util.find_spec(module) is not None
    for module in ("numpy", "pandas", "plotly")
)


@unittest.skipUnless(
    DASHBOARD_DEPENDENCIES_AVAILABLE,
    "dashboard dependencies are not installed",
)
class DashboardTests(unittest.TestCase):
    def test_load_snapshot_filters_and_normalizes_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "event.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(
                    output,
                    fieldnames=["tick", "adc_value", "wire_id", "tpc", "plane"],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {"tick": 0, "adc_value": 100, "wire_id": 2, "tpc": 0, "plane": 0},
                        {"tick": 1, "adc_value": 110, "wire_id": 2, "tpc": 0, "plane": 0},
                        {"tick": 0, "adc_value": 200, "wire_id": 4, "tpc": 0, "plane": 0},
                        {"tick": 1, "adc_value": 220, "wire_id": 4, "tpc": 0, "plane": 0},
                        {"tick": 0, "adc_value": 999, "wire_id": 8, "tpc": 1, "plane": 0},
                    ]
                )

            snapshot = load_snapshot(csv_path, tpc=0, plane=0)

            self.assertEqual(snapshot.rows, 4)
            self.assertEqual(snapshot.wires, 2)
            self.assertEqual((snapshot.min_tick, snapshot.max_tick), (0, 1))
            self.assertEqual(list(snapshot.figure.data[0].y), [2, 4])

    def test_load_snapshot_rejects_missing_filter_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "event.csv"
            csv_path.write_text(
                "tick,adc_value,wire_id,tpc,plane\n0,100,2,0,0\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "No complete rows"):
                load_snapshot(csv_path, tpc=1, plane=0)


if __name__ == "__main__":
    unittest.main()
