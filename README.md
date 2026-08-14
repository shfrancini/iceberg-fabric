# FABRIC Packet Visualization

This repository demonstrates a packet-to-visualization workflow across FABRIC.
It receives detector packets from either a capture file or a live network
interface, converts them into mapped CSV rows, and updates a heatmap while data
is arriving.

The implementation extends WibViz with a non-interactive export path and the
supporting tools needed to run and validate the workflow across multiple FABRIC
nodes.

## Project structure

- `wibviz_graph/` contains the Python package, command-line tools, tests,
  mapping resources, and demonstration scripts.
- `graph_notebook/` contains notebooks for generating heatmaps and checking
  replayed output against reference data.

## Main additions

- CSV export from packet captures and live network interfaces
- Incremental output for visualizing data during capture
- A live Plotly dashboard and a lightweight delta server
- Scripts for replay, capture, synchronization, snapshots, and validation
- Automated tests for export, mapping, dashboard, and delta-update behavior

## Installation

From the repository root:

```bash
cd wibviz_graph
python -m pip install -e .
```

Install the optional dashboard dependencies with:

```bash
python -m pip install -e '.[dashboard]'
```

Packet-to-wire mapping also requires a configured DAQ software environment.
The relevant setup notes and usage instructions are in `wibviz_graph/docs/`.

## Demonstration and validation

The demonstrated workflow sends a known packet capture across FABRIC, receives
and decodes the packets on another node, writes CSV rows incrementally, and
refreshes a heatmap during the run.

For the recorded validation run, the receiver processed all 640 transmitted
packets without a reported skip. Those packets produced 409,600 CSV rows, and
the generated values matched the reference CSV row for row. These figures
describe that specific controlled replay; they are not general performance
benchmarks.

## Acknowledgment

This work began with a repository and project context shared by **Kels Cavin**.
The underlying visualization software is available in the
[original WibViz repository](https://github.com/dingp/wibwiz).
