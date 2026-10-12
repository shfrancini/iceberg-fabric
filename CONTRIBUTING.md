# Contributing

Contributions should preserve the public baseline contract unless a change is
explicitly documented as experimental.

## Development Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -e '.[dev,dashboard,notebook]'
```

## Baseline Rules

- Keep the default path single-decoder.
- Keep `chunk_packets: 64`, `capacity_chunks: 4`, and
  `persistent_batch_chunks: 1` in the public baseline template.
- Do not commit packet captures, private credentials, private slice names,
  generated outputs, virtual environments, or cache directories.
- Do not make high-rate stress runs the default examples.
- Do not add code paths that modify FABRIC slices from the pipeline runner.

## Required Checks

Run these before opening a change:

```bash
python3 -m py_compile iceberg_fabric/*.py scripts/*.py wibviz_graph/src/wibviz/*.py
python3 -m pytest wibviz_graph/tests
python3 scripts/validate_persistent_multichunk_batching_no_replay.py
iceberg-fabric preflight --config configs/single_decoder_baseline_template.yaml --local-only
iceberg-fabric run --config configs/single_decoder_baseline_template.yaml --dry-run
iceberg-fabric demo --output /tmp/iceberg_fabric_demo_check
```

Remote replay and smoke tests require user-provided FABRIC resources and data.
Do not run them as part of ordinary local validation.

## Documentation

Update README and reference docs whenever a config key, output field, or command
interface changes. User-facing examples must use placeholders or clearly marked
example paths rather than private active-slice paths.
