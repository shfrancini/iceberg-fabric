# Initial Bottleneck Evidence

Created: 2026-07-20

Source report: `rebuild_restore/reports/scalability_2026-07-07/event_6860_scalability_progress_report_20260707.md`

CSV table: `rebuild_restore/artifacts/final_presentation_evidence_2026-07-20/initial_bottleneck_15_20_25pps_table.csv`

## Slide Claim

Original synchronous receiver/export path failed starting around 20 pps.

## Evidence Table

| rate_pps | sender_result | receiver_export_result | validation_result | meaning |
| ---: | --- | --- | --- | --- |
| 15 | 640/640 sent | 640/640 read | PASS, 0 missing / 0 extra | Existing pipeline still kept up |
| 20 | 640/640 sent | 537/640 read | FAIL, 391,680 missing / 304,128 extra | Receiver/export path fell behind |
| 25 | 640/640 sent | 434/640 read | FAIL | Shortfall worsened as rate increased |

## Slide-Ready Explanation

At 20 pps, tcpreplay still reported all 640 packets sent successfully, but the receiver/export path only processed 537 packets before timeout. The generated CSV was incomplete and failed exact validation, so the bottleneck was the coupled receiver/export path, not sender replay.

## Caution

Do not describe this as proven network packet loss. The evidence supports receiver/export shortfall or timeout before the full event was processed.
