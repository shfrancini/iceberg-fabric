# Legacy Demo Guide Alias

This repository now exposes the public baseline through the `iceberg-fabric`
CLI. Use the root [README.md](../README.md) for the current workflow.

Current entry points:

```bash
iceberg-fabric demo --output /tmp/iceberg_fabric_readme_demo
iceberg-fabric preflight --config configs/single_decoder_baseline_template.yaml --local-only
iceberg-fabric run --config configs/single_decoder_baseline_template.yaml --dry-run
```

The historical `wib_fabric_live_demo` name appears in older evidence and
notebooks. New public instructions should use `iceberg-fabric` and
`configs/single_decoder_baseline_template.yaml`.
