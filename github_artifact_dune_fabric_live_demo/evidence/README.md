# Evidence

This directory contains compact evidence only. It intentionally excludes PCAPs, full decoded event CSVs, raw chunks, and verbose logs.

Included highlights:

- `initial_bottleneck_15_20_25pps_table.csv`: initial synchronous receiver/export bottleneck evidence.
- `queue_v2_stress_matrix_15000_30000pps.csv`: Queue v2 stress matrix through 30,000 pps.
- `event_4245_live_visualization_timing.csv`: live visualization timing/lag table from the latest local event 4245 presentation run.
- `event_4245_chunk_coverage_table.csv`: explains why packet-chunk live visualization fills by wire/y-axis sections rather than left-to-right in tick/time.
- `event_4245_chunk_wire_tick_coverage.png`: simple plot of per-chunk wire ranges.
- `original_benchmark_4245.md`: provenance for benchmark heatmap/ridgeline figures.
