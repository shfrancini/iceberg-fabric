# FABRIC Packet Visualization

This project explores a live packet-to-visualization workflow on FABRIC. It
extends WibViz with a headless CSV exporter, live capture and replay helpers,
validation tools, and notebooks for following detector data as it arrives.

The work began from a repository shared by **Kels Cavin** and builds on the
original [WibViz project](https://github.com/dingp/wibwiz). Thank you to Kels
for the starting point and project context. The changes here focus on making
the existing packet-processing path reproducible in a distributed FABRIC
environment.

## Repository layout

- `wibviz_graph/` contains the Python package, command-line tools, tests, and
  the live demonstration runbook.
- `graph_notebook/` contains notebooks for plotting exported CSV data and
  checking live replay results.

## What was added

- A command-line exporter for converting packet captures or live traffic into
  graph-ready CSV rows.
- A live dashboard and a lightweight delta server for incremental updates.
- Sender, receiver, synchronization, snapshot, and validation scripts for a
  multi-node FABRIC demonstration.
- Tests for the exporter, dashboard, delta server, and graph-data extraction
  path.

## Getting started

Install the package from the project directory:

```bash
cd wibviz_graph
python -m pip install -e .
```

Dashboard support is optional:

```bash
python -m pip install -e '.[dashboard]'
```

The packet-to-wire mapping tools require a separately configured DAQ software
environment. Setup notes and usage examples are available under
`wibviz_graph/docs/`.

## Validation status

The current demonstration replays a known packet capture through FABRIC,
captures it on a receiver, writes CSV output incrementally, and refreshes a
heatmap during the run. In the recorded validation run, all 640 packets were
parsed without skips and the resulting 409,600 rows matched the reference CSV
value for value.

## Attribution

This is a modified research and demonstration repository. Credit for the
starting repository and early project work belongs to **Kels Cavin**. WibViz,
the underlying packet visualization project, is available from its
[original repository](https://github.com/dingp/wibwiz).
