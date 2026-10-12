# Installation Guide

## Clean Clone Install

Use this command from the repository root when you want the CLI, tests,
dashboard dependencies, and notebook dependencies available:

```bash
python3 -m pip install -e '.[dev,dashboard,notebook]'
```

This installs:

- `iceberg-fabric`, the public CLI wrapper
- WibViz package modules from `wibviz_graph/src`
- `PyYAML`, `pandas`, `numpy`, `matplotlib`, and `scapy`
- `pytest` for local tests
- `dash` and `plotly` for dashboard workflows
- `jupyterlab`, `ipykernel`, and `nbformat` for notebook workflows

For a smaller runtime-only install, use:

```bash
python3 -m pip install -e .
```

## Command Interface

After installation, the public interface is:

```bash
iceberg-fabric preflight --config CONFIG
iceberg-fabric run --config CONFIG
iceberg-fabric demo --output OUTPUT_DIR
```

`iceberg-fabric run` is a wrapper around `scripts/run_pipeline.py`. The wrapper
keeps the public command stable while preserving the current source layout.

## VM Requirements

The local package install does not configure sender or receiver VMs. For replay
mode, install `tcpreplay` on the sender and `tcpdump` plus Python dependencies
on the receiver. The receiver also needs access to the WibViz source path,
mapping CSV, and optional validation reference CSV listed in the config.

## Optional Dependencies

Install only what you need:

```bash
python3 -m pip install -e '.[dev]'
python3 -m pip install -e '.[dashboard]'
python3 -m pip install -e '.[notebook]'
```

The baseline runner requires `PyYAML`, `pandas`, `numpy`, `matplotlib`, and
`scapy`. Dashboard and notebook extras are not required for a non-interactive
pipeline run.
