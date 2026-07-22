# Queue V2 Stress Matrix - Event 6860

Date: 2026-07-14

Active slice: `dune-udp-demo-rebuild-2`

Queue architecture: `queue_overlapped_decode_v2`

Settings: event 6860, packet limit 640, chunk-packets 64, mapping `/home/ubuntu/wibviz_inputs/event-4245-channel-map.csv`, receiver root `/home/ubuntu`, persistent root `/tmp/wibviz-queue-live`.

## Result

Queue v2 passed every requested run above 10,000 pps: 15,000, 20,000, and 30,000 pps. No first failing pps was found in this matrix.

Highest passing Queue v2 pps in this run: `30,000`.

## Queue V2 Matrix

| pps | result | sent ok | captured | drops | decoded pkts | decoded rows | validation | missing / extra | raw chunks | decoded chunks | max queue depth | final depth | max oldest age s | max lag chunks / s | runtime s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | pass | 640 | 640 | 0 | 640 | 491520 | pass | 0 / 0 | 10 | 10 | 2 | 0 | 52.217 | 1 / 56.655 | 338.103 |
| 15,000 | pass | 640 | 640 | 0 | 640 | 491520 | pass | 0 / 0 | 10 | 10 | 2 | 0 | 30.545 | 1 / 33.003 | 286.773 |
| 20,000 | pass | 640 | 640 | 0 | 640 | 491520 | pass | 0 / 0 | 10 | 10 | 2 | 0 | 34.804 | 1 / 37.582 | 297.913 |
| 30,000 | pass | 640 | 640 | 0 | 640 | 491520 | pass | 0 / 0 | 10 | 10 | 2 | 0 | 32.515 | 1 / 34.851 | 294.899 |

## Parallel Baseline Comparison

The parallel baseline is the existing post-fix matrix from `/tmp/wibviz-postfix-cap-20260714`. It also passed through 30,000 pps.

| pps | Queue v2 max lag chunks / s | Queue v2 runtime s | Parallel max lag chunks / s | Parallel runtime s |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 1 / 56.655 | 338.103 | 1 / 34.285 | 302.578 |
| 15,000 | 1 / 33.003 | 286.773 | 1 / 34.196 | 313.13 |
| 20,000 | 1 / 37.582 | 297.913 | 1 / 34.656 | 296.27 |
| 30,000 | 1 / 34.851 | 294.899 | 1 / 37.541 | 299.256 |

## Interpretation

Queue v2 remains the preferred live architecture. It preserved raw chunks first, decoded all chunks, returned queue depth to 0, passed validation, and exposed queue/backpressure metrics at every tested pps. The parallel runner is still a useful latency/runtime baseline, but it does not provide the same explicit raw-preservation queue contract.

The 30,000 pps result did not expose packet loss, tcpdump drops, validation mismatch, ordering failure, or queue buildup. The remaining bottleneck is still orchestration/decode/export/runtime overhead rather than packet capture correctness for this 640-packet event.

## Next Matrix

1. Run chunk-size comparison at 30,000 pps: chunk-packets 32 and 128, packet limit 640.
2. If both pass, test 50,000 pps with chunk-packets 64 and the same stop-on-failure rule.
3. Track viewer/render lag if a live watcher is attached, because backend correctness is now ahead of UI/render characterization.

## Artifacts

- compact CSV: `/home/fabric/work/visualization-wibwiz/rebuild_restore/artifacts/queue_v2_2026-07-14/event_6860_queue_v2_stress_matrix_15000_30000pps.csv`
- compact JSON: `/home/fabric/work/visualization-wibwiz/rebuild_restore/artifacts/queue_v2_2026-07-14/event_6860_queue_v2_stress_matrix_15000_30000pps.json`
- source Queue v2 summaries: `/tmp/wibviz-queue-live/reports/queue_capture_decode_2026-07-14/queue-v2d-6860-640pkt-<PPS>pps/summary.json`
- source Queue v2 manifests: `/tmp/wibviz-queue-live/reports/queue_capture_decode_2026-07-14/queue-v2d-6860-640pkt-<PPS>pps/queue_manifest.json`
