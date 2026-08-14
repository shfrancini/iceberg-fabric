# Artifact Manifest

This manifest describes the material included in the repository. Temporary
runtime output, packet captures, caches, and local environments are excluded by
`.gitignore`.

## Core implementation

| Path | Contents |
| --- | --- |
| `wibviz_graph/src/wibviz/` | Packet parsing, channel mapping, CSV export, dashboard, delta server, and terminal display code |
| `wibviz_graph/tests/` | Automated tests for export, mapping, dashboard snapshots, and delta updates |
| `scripts/run_pipeline.py` | End-to-end workflow orchestration |
| `scripts/remote_transport.py` | File transfer between FABRIC nodes |
| `scripts/live_chunk_viewer.py` | Incremental visualization of arriving CSV chunks |
| `scripts/validate_csv_against_reference.py` | Comparison of generated CSV output with reference data |

## Supporting utilities

- `scripts/build_pipeline_plan_pdf.py` builds a project-planning PDF from an
  external capacity-summary dataset; the dataset is not included.
- `wibviz_graph/pyproject.toml` defines the Python package, dependencies,
  optional dashboard dependencies, and command-line entry points.
- `wibviz_graph/scripts/` contains mapping-environment setup helpers and the
  sender, receiver, synchronization, snapshot, and validation commands used by
  the multi-node demonstration.
- `wibviz_graph/wibviz-screen-cap.gif` is the original terminal demonstration
  linked from the package documentation.

## Configuration

The files under `configs/` define capture-file replay, live-interface input,
remote execution, and custom-input templates. They are examples and must be
reviewed for the target FABRIC slice before use.

## Notebooks

| Path | Purpose |
| --- | --- |
| `notebooks/00_RUN_DEMO.ipynb` | Guided end-to-end demonstration |
| `notebooks/event_4245_live_demo_presentation.ipynb` | Presentation-oriented visualization workflow |
| `graph_notebook/generate_graph.ipynb` | Graph generation from exported CSV data |
| `graph_notebook/live_delta_viewer.ipynb` | Incremental updates from the delta server |
| `graph_notebook/live_replay_viewer.ipynb` | Visualization during packet replay |
| `graph_notebook/validate_live_replay.ipynb` | Replay-output validation |

## Documentation

- `README.md` is the public project overview.
- `REPRODUCIBILITY.md` records the reproducibility workflow.
- `docs/` contains setup, architecture, transport, live-input, and
  troubleshooting guides.
- `wibviz_graph/README.md` and `wibviz_graph/docs/` document the Python package,
  mapping environment, and multi-node demonstration.

## Evidence and figures

The `evidence/` directory contains compact tables, reports, and run summaries
used to support the documented validation results. The `figures/` directory
contains the corresponding heatmap, waveform, and coverage images.

Two reference CSV files are included under
`wibviz_graph/docs/historic_csv_outputs/simulated/`. They are approximately
34 MB and 13 MB and are retained to support row-by-row validation.

Mapping resources used by the graph-data extraction path are included under
`wibviz_graph/docs/raw_mappings/`.

## Intentionally excluded

| Pattern | Reason |
| --- | --- |
| `*.pcap`, `*.pcapng` | Raw captures are large runtime inputs |
| `tmp/`, `output/` | Scratch files and generated review artifacts |
| `__pycache__/`, `*.pyc` | Generated Python bytecode |
| `.venv/`, build directories | Local environments and package build output |
| `replay-*.csv` | Regenerable runtime exports |
| Office lock files | Local editor artifacts |

## Verification note

The recorded controlled replay processed 640 packets into 409,600 CSV rows
without a reported skip, and the generated values matched the reference CSV
row for row. These figures describe that recorded run and should not be treated
as general performance benchmarks.
