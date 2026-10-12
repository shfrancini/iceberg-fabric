from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path

from . import __version__


def _scripts_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "scripts"


def _ensure_scripts_importable() -> None:
    scripts_dir = _scripts_dir()
    if scripts_dir.is_dir():
        sys.path.insert(0, str(scripts_dir))


def _run_pipeline(argv: list[str]) -> int:
    _ensure_scripts_importable()
    module = importlib.import_module("run_pipeline")
    return int(module.main(argv))


def _build_preflight_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="iceberg-fabric preflight", description="Validate config and environment without packet replay.")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--local-only", action="store_true", help="Do not contact sender or receiver hosts.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument("--timeout", type=int, default=15, help="Remote command timeout in seconds.")
    return parser


def _print_help() -> None:
    print("usage: iceberg-fabric [--version] {preflight,run,demo} ...")
    print()
    print("commands:")
    print("  preflight   validate config and environment without packet replay")
    print("  run         run or dry-run the packet pipeline")
    print("  demo        generate a local visualization-only demo")


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in {"-h", "--help"}:
        _print_help()
        return 0
    if args[0] == "--version":
        print(f"iceberg-fabric {__version__}")
        return 0
    command, rest = args[0], args[1:]
    if command == "run":
        if rest and rest[0] == "--":
            rest = rest[1:]
        if not rest:
            print("iceberg-fabric run requires arguments such as --config CONFIG", file=sys.stderr)
            return 2
        return _run_pipeline(rest)
    if command == "demo":
        from .demo import main as demo_main

        return demo_main(rest)
    if command == "preflight":
        from .preflight import run_preflight

        return run_preflight(_build_preflight_parser().parse_args(rest))
    print(f"unknown command: {command}", file=sys.stderr)
    _print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
