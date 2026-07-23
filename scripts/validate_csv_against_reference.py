#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

DEFAULT_EXCLUDED_FIELDS = {"packet_file"}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def line_count(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for _ in handle)


def csv_fields(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle).fieldnames or [])


def parse_compare_fields(value: str | None, output: Path, reference: Path) -> list[str]:
    output_fields = csv_fields(output)
    reference_fields = csv_fields(reference)
    if value:
        fields = [item.strip() for item in value.split(",") if item.strip()]
    else:
        reference_set = set(reference_fields)
        fields = [
            field
            for field in output_fields
            if field in reference_set and field not in DEFAULT_EXCLUDED_FIELDS
        ]
    if not fields:
        raise ValueError("no comparable CSV fields were found")
    missing_output = [field for field in fields if field not in output_fields]
    missing_reference = [field for field in fields if field not in reference_fields]
    if missing_output or missing_reference:
        raise ValueError(
            "missing compare fields: "
            + json.dumps({"output": missing_output, "reference": missing_reference}, sort_keys=True)
        )
    return fields


def projected_counter(path: Path, fields: list[str]) -> tuple[Counter[tuple[str, ...]], int]:
    counts: Counter[tuple[str, ...]] = Counter()
    rows = 0
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            counts[tuple((row.get(field) or "").strip() for field in fields)] += 1
            rows += 1
    return counts, rows


def examples(counter: Counter[tuple[str, ...]], fields: list[str], limit: int = 5) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, count in counter.items():
        rows.append({field: value for field, value in zip(fields, key)} | {"count": count})
        if len(rows) >= limit:
            break
    return rows


def compare(output: Path, reference: Path, fields: list[str]) -> dict[str, Any]:
    generated, generated_rows = projected_counter(output, fields)
    expected, reference_rows = projected_counter(reference, fields)
    missing = expected - generated
    extra = generated - expected
    return {
        "generated_rows_compared": generated_rows,
        "reference_rows_compared": reference_rows,
        "missing_row_count": sum(missing.values()),
        "extra_row_count": sum(extra.values()),
        "missing_examples": examples(missing, fields),
        "extra_examples": examples(extra, fields),
        "pass": not missing and not extra,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate generated WIBViz CSV against an optional reference CSV.")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--compare-fields", default=None)
    return parser


def validate(args: argparse.Namespace) -> dict[str, Any]:
    output = args.output.expanduser()
    reference = args.reference.expanduser()
    if not output.is_file():
        raise FileNotFoundError(f"generated CSV not found: {output}")
    if not reference.is_file():
        raise FileNotFoundError(f"reference CSV not found: {reference}")

    fields = parse_compare_fields(args.compare_fields, output, reference)
    generated_lines = line_count(output)
    reference_lines = line_count(reference)
    comparison = compare(output, reference, fields)
    report = {
        "validation_type": "generic_csv_reference_comparison",
        "output": str(output),
        "output_sha256": sha256(output),
        "reference": str(reference),
        "reference_sha256": sha256(reference),
        "generated_lines_including_header": generated_lines,
        "generated_rows": max(0, generated_lines - 1),
        "reference_lines_including_header": reference_lines,
        "reference_rows": max(0, reference_lines - 1),
        "compare_fields": fields,
        "comparison": comparison,
        "pass": (
            generated_lines == reference_lines
            and comparison["pass"]
            and comparison["generated_rows_compared"] == comparison["reference_rows_compared"]
        ),
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.report.with_suffix(args.report.suffix + ".tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(args.report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return report


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = validate(args)
    except Exception as error:
        print(f"validate_csv_against_reference: error: {error}")
        return 1
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

