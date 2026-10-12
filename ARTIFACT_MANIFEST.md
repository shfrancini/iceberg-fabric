# Artifact Manifest

This manifest describes the public baseline repository contents. Runtime output,
packet captures, caches, and local environments are excluded by `.gitignore`.

## Core Implementation

| Path | Contents |
| --- | --- |
| `iceberg_fabric/` | Public CLI wrapper, local synthetic demo, and no-replay preflight checks. |
| `scripts/run_pipeline.py` | Single-decoder replay/capture/decode/output orchestration. |
| `scripts/receiver_decode_export_worker.py` | Persistent receiver-side WibViz decode/export worker. |
| `scripts/remote_transport.py` | LoomAI and SSH transport abstraction. |
| `scripts/validate_csv_against_reference.py` | Exact generated-vs-reference CSV comparison helper. |
| `scripts/validate_persistent_multichunk_batching_no_replay.py` | No-replay batching/controller validation helper. |
| `wibviz_graph/src/wibviz/` | WibViz parsing, mapping, export, display, dashboard, and graph extraction modules. |
| `wibviz_graph/tests/` | Public unit tests, including focused persistent-worker and ordering tests. |

## Configuration

| Path | Purpose |
| --- | --- |
| `configs/single_decoder_baseline_template.yaml` | Portable default template for the public baseline. |
| `configs/event_6860_finite_benchmark_example.yaml` | User-provided finite benchmark example, not bundled data. |
| `configs/event_4245_legacy_example.yaml` | Historical event 4245 example retained for comparison. |

Older config files may remain for historical compatibility, but new workflows
should start from `single_decoder_baseline_template.yaml`.

## Documentation

| Path | Purpose |
| --- | --- |
| `README.md` | Product overview and four workflows. |
| `docs/installation.md` | Clean clone installation and dependency guidance. |
| `docs/configuration_reference.md` | YAML field reference. |
| `docs/output_schema.md` | Output directory and metric schema. |
| `docs/troubleshooting.md` | Preflight-driven repair guidance. |
| `docs/architecture.md` | Stable baseline architecture. |

## Notebooks And Historical Evidence

The notebooks, `evidence/`, and `figures/` directories contain compact public
artifacts from earlier visualization and validation work. They are useful for
understanding the project history but are not required for the current CLI
workflow.

No large PCAP files are bundled. Users must provide replay PCAPs, mapping CSVs,
and validation reference CSVs for real runs.

## Intentionally Excluded

| Pattern | Reason |
| --- | --- |
| `*.pcap`, `*.pcapng` | Raw captures are large user-provided runtime inputs. |
| `tmp/`, `output/` | Scratch files and generated review artifacts. |
| `__pycache__/`, `*.pyc` | Generated Python bytecode. |
| `.venv/`, build directories | Local environments and build products. |
| `replay-*.csv` | Regenerable runtime exports. |
