# Output Schema

The runner writes local artifacts under:

```text
<output_root>/runs/<run_id>/
  artifacts/
  reports/
  visualizations/
```

The default `output_root` comes from `paths.output_root`. It can be overridden
with `iceberg-fabric run --output-root PATH`.

## Artifacts

| Path | Format | Description |
| --- | --- | --- |
| `artifacts/combined.csv` | CSV | Ordered decoded rows appended from decoded chunks. |
| `artifacts/decoded_chunks/` | CSV files | Local mirror of per-chunk decoded CSV outputs when available. |

`combined.csv` uses the WibViz export field order:

```text
seq_id,timestamp,tick,adc_value,wire_id,tpc,slot,plane,packet_file
```

## Reports

| Path | Format | Description |
| --- | --- | --- |
| `reports/summary.json` | JSON | Top-level run status, paths, validation, visualization, and metrics. |
| `reports/manifest.json` | JSON | Incremental manifest written while the run advances through phases. |
| `reports/measurement_summary.csv` | CSV | One-row metric summary for spreadsheet comparison. |
| `reports/queue_metrics_history.csv` | CSV | Queue depth and backlog samples over time. |
| `reports/validation_result.json` | JSON | Exact validation result when validation is enabled. |
| `reports/dry_run.json` | JSON | Planned commands and placeholder metrics for dry-runs. |

Repeated runs also write:

| Path | Format | Description |
| --- | --- | --- |
| `reports/repeated_replay_summary.json` | JSON | Aggregate status across repeats. |
| `reports/repeated_replay_metrics.csv` | CSV | One row per repeat. |

## Required Metrics

The baseline reports these metric groups:

- Throughput: validated packets per second and validated rows per second.
- Latency: capture-to-output latency summary and phase timing.
- Queue depth: raw queue depth, writer queue depth, final depth, and whether
  queues returned to zero.
- Backpressure: blocked put count and total blocked seconds for raw and writer
  queues.
- Decode: worker startup, request/response, worker processing, packet counts,
  skipped packets, and worker failures.
- Batch: requested batch size, actual batch size distribution, singleton and
  partial batch counts, and multi-chunk batch count.
- Ordering: chunk submission/result/output exactly-once checks and ordered
  append records.
- Validation: exact comparison status, return code, expected packets, and
  expected rows.

Dry-runs emit placeholder metric fields marked with `dry_run_placeholder` so
schemas can be checked without replaying packets.

## Visualizations

When `visualization.enabled` is true, the run writes:

| Path | Description |
| --- | --- |
| `visualizations/heatmap.png` | Wire by tick ADC heatmap. |
| `visualizations/stacked_waveform.png` | Sampled stacked waveform preview. |

The local synthetic demo writes similarly named files under its selected output
directory and does not use the run directory layout.
