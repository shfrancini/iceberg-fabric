from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="iceberg-fabric demo", description="Generate a local visualization-only demonstration.")
    parser.add_argument("--output", type=Path, default=Path("output/local_demo"), help="Output directory for generated demo files.")
    parser.add_argument("--ticks", type=int, default=64, help="Number of synthetic ticks.")
    parser.add_argument("--wires", type=int, default=32, help="Number of synthetic wires.")
    return parser


def run_demo(args: argparse.Namespace) -> int:
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    ticks = np.arange(args.ticks)
    wires = np.arange(args.wires)
    rows = []
    for wire in wires:
        phase = wire / max(args.wires, 1) * np.pi
        waveform = 1200 + 140 * np.sin((ticks / max(args.ticks, 1) * 2 * np.pi) + phase) + wire * 2
        for tick, adc in zip(ticks, waveform):
            rows.append(
                {
                    "seq_id": int(tick // 8),
                    "timestamp": int(tick * 2048),
                    "tick": int(tick),
                    "adc_value": round(float(adc), 3),
                    "wire_id": int(wire),
                    "tpc": 0,
                    "slot": int(wire % 4),
                    "plane": 2,
                    "packet_file": "synthetic-local-demo",
                }
            )
    frame = pd.DataFrame(rows)
    csv_path = output / "synthetic_wib_rows.csv"
    frame.to_csv(csv_path, index=False)

    pivot = frame.pivot(index="wire_id", columns="tick", values="adc_value")
    heatmap_path = output / "synthetic_heatmap.png"
    plt.figure(figsize=(10, 5))
    plt.imshow(pivot.values, aspect="auto", cmap="viridis", origin="lower")
    plt.colorbar(label="ADC")
    plt.xlabel("Tick")
    plt.ylabel("Wire")
    plt.title("Synthetic ICEBERG/WIB Heatmap")
    plt.tight_layout()
    plt.savefig(heatmap_path, dpi=150)
    plt.close()

    waveform_path = output / "synthetic_waveforms.png"
    plt.figure(figsize=(10, 5))
    for wire in range(min(8, args.wires)):
        subset = frame[frame["wire_id"] == wire]
        plt.plot(subset["tick"], subset["adc_value"] + wire * 40, label=f"wire {wire}")
    plt.xlabel("Tick")
    plt.ylabel("ADC plus display offset")
    plt.title("Synthetic Waveform Preview")
    plt.tight_layout()
    plt.savefig(waveform_path, dpi=150)
    plt.close()

    summary = {
        "status": "PASS",
        "rows": int(len(frame)),
        "ticks": int(args.ticks),
        "wires": int(args.wires),
        "csv": str(csv_path),
        "heatmap": str(heatmap_path),
        "waveforms": str(waveform_path),
    }
    summary_path = output / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    return run_demo(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
