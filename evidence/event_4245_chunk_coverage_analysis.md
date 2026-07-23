# Event 4245 Chunk Coverage Analysis

Created: 2026-07-22

## Data Source

- Latest `/tmp` event 4245 live run with decoded chunks: `/tmp/wib_fabric_live_demo/runs/presentation-live-4245-20260722T153315Z`
- Decoded chunk directory: `/tmp/wib_fabric_live_demo/runs/presentation-live-4245-20260722T153315Z/artifacts/decoded_chunks`
- Chunk CSV count: 10
- Empty decoded chunks for this selected event/plane: `[2, 3, 6, 7, 9]`
- Full decoded event wire range in non-empty chunks: `0-199` (200 unique wires)
- Full decoded event tick range in non-empty chunks: `0-2047` (2048 unique ticks)

No replay was rerun. This analysis uses existing decoded chunk CSVs under `/tmp`.

## Central Answer

The current live plot fills by wire/y-axis sections because the live chunks are packet batches, not tick/time image slices. In this event 4245 run, each non-empty packet chunk covers the broad tick range `0-2047` but only a subset of the wire range. When each cumulative packet-batch prefix is plotted in heatmap coordinates, newly arrived rows land in their decoded `wire_id`/`tick` positions, so the image appears to fill by wire regions. A left-to-right animation would be a tick-based playback or reveal of an already decoded image, not the actual chunk arrival order.

## Coverage Summary

- Classification counts: `{"empty decoded chunk for selected event/plane": 5, "wire-banded chunk: broad tick coverage, partial wire coverage": 5}`
- Every non-empty chunk spans the full tick min/max range: `True`
- Every non-empty chunk has the full unique tick count: `True`
- Every non-empty chunk has only a partial wire subset: `True`

|   chunk_index |   row_count | wire_id_min   | wire_id_max   |   unique_wire_count | tick_min   | tick_max   |   unique_tick_count |   wire_coverage_fraction_of_event |   tick_coverage_fraction_of_event | coverage_classification                                       |
|--------------:|------------:|:--------------|:--------------|--------------------:|:-----------|:-----------|--------------------:|----------------------------------:|----------------------------------:|:--------------------------------------------------------------|
|             0 |       81920 | 160           | 199           |                  40 | 0          | 2047       |                2048 |                               0.2 |                                 1 | wire-banded chunk: broad tick coverage, partial wire coverage |
|             1 |       81920 | 120           | 159           |                  40 | 0          | 2047       |                2048 |                               0.2 |                                 1 | wire-banded chunk: broad tick coverage, partial wire coverage |
|             2 |           0 | <NA>          | <NA>          |                   0 | <NA>       | <NA>       |                   0 |                               0   |                                 0 | empty decoded chunk for selected event/plane                  |
|             3 |           0 | <NA>          | <NA>          |                   0 | <NA>       | <NA>       |                   0 |                               0   |                                 0 | empty decoded chunk for selected event/plane                  |
|             4 |       81920 | 80            | 119           |                  40 | 0          | 2047       |                2048 |                               0.2 |                                 1 | wire-banded chunk: broad tick coverage, partial wire coverage |
|             5 |       81920 | 40            | 79            |                  40 | 0          | 2047       |                2048 |                               0.2 |                                 1 | wire-banded chunk: broad tick coverage, partial wire coverage |
|             6 |           0 | <NA>          | <NA>          |                   0 | <NA>       | <NA>       |                   0 |                               0   |                                 0 | empty decoded chunk for selected event/plane                  |
|             7 |           0 | <NA>          | <NA>          |                   0 | <NA>       | <NA>       |                   0 |                               0   |                                 0 | empty decoded chunk for selected event/plane                  |
|             8 |       81920 | 0             | 39            |                  40 | 0          | 2047       |                2048 |                               0.2 |                                 1 | wire-banded chunk: broad tick coverage, partial wire coverage |
|             9 |           0 | <NA>          | <NA>          |                   0 | <NA>       | <NA>       |                   0 |                               0   |                                 0 | empty decoded chunk for selected event/plane                  |

## Older GIF / Tick Playback Evidence

Existing evidence in `rebuild_restore/reports/final_presentation_evidence_2026-07-20/actual_replay_gif_ordering_check.md` says the fixed-vs-adaptive GIF was generated from cumulative per-chunk decoded CSV outputs, not from a final CSV left-to-right mask. The same report notes that heatmaps do not need to fill left-to-right because packet/chunk order is not guaranteed to correspond to increasing tick position.

The current `wib_fabric_live_demo/00_RUN_DEMO.ipynb` also contains an optional helper named `visual_playback_by_tick(...)` whose markdown/source explicitly labels decoded tick-position playback as separate from live packet chunk progress. I did not find evidence in the inspected repo files that the event 4245 live demo should use a tick-based reveal for live chunk mode.

## Artifacts

- Coverage CSV: `/home/fabric/work/visualization-wibwiz/rebuild_restore/artifacts/final_presentation_evidence_2026-07-22/event_4245_chunk_coverage_table.csv`
- Wire coverage plot: `/home/fabric/work/visualization-wibwiz/rebuild_restore/artifacts/final_presentation_evidence_2026-07-22/event_4245_chunk_wire_tick_coverage.png`
