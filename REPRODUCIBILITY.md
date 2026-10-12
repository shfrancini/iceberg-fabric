# Reproducibility

This file records the public no-replay and user-provided replay workflows. The
repository does not include large PCAP inputs or private slice configuration.

## Install

```bash
python3 -m pip install -e '.[dev,dashboard,notebook]'
```

## Local Checks Without FABRIC

```bash
python3 -m py_compile iceberg_fabric/*.py scripts/*.py wibviz_graph/src/wibviz/*.py
python3 -m pytest wibviz_graph/tests
python3 scripts/validate_persistent_multichunk_batching_no_replay.py
iceberg-fabric preflight --config configs/single_decoder_baseline_template.yaml --local-only
iceberg-fabric run --config configs/single_decoder_baseline_template.yaml --dry-run
iceberg-fabric demo --output /tmp/iceberg_fabric_readme_demo
```

These commands do not replay packets and do not contact FABRIC nodes when
`--local-only` and `--dry-run` are used as shown.

## User-Provided Replay

Copy and edit the baseline template:

```bash
cp configs/single_decoder_baseline_template.yaml configs/my_baseline.yaml
```

Run preflight against the real sender and receiver:

```bash
iceberg-fabric preflight --config configs/my_baseline.yaml
```

Run a bounded smoke test:

```bash
iceberg-fabric run --config configs/my_baseline.yaml --max-live-chunks 2
```

Run the full configured replay only after the smoke test validates the selected
PCAP, mapping, reference CSV, interfaces, and output paths:

```bash
iceberg-fabric run --config configs/my_baseline.yaml
```

## Historical Evidence

The `evidence/` and `figures/` directories contain compact historical artifacts
from recorded runs. They document observed behavior for those runs only and are
not general throughput guarantees.
