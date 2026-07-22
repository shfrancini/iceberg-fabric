# Artifact Manifest

Artifact root: `/home/fabric/work/visualization-wibwiz/github_artifact_dune_fabric_live_demo`

This directory was created by copying essential files only. Large runtime products, PCAPs, full decoded CSVs, caches, checkpoints, and virtual environments are omitted.

## Included Files

| File | Why included | Source |
| --- | --- | --- |
| `notebooks/event_4245_live_demo_presentation.ipynb` | focused event 4245 presentation/live visualization notebook (outputs stripped) | `/home/fabric/work/visualization-wibwiz/graph_notebook/event_4245_live_demo_presentation.ipynb` |
| `notebooks/00_RUN_DEMO.ipynb` | Start Here end-to-end demo notebook (outputs stripped) | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/00_RUN_DEMO.ipynb` |
| `scripts/run_pipeline.py` | core runner/viewer/transport/validation code | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/scripts/run_pipeline.py` |
| `scripts/live_chunk_viewer.py` | core runner/viewer/transport/validation code | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/scripts/live_chunk_viewer.py` |
| `scripts/remote_transport.py` | core runner/viewer/transport/validation code | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/scripts/remote_transport.py` |
| `scripts/validate_csv_against_reference.py` | core runner/viewer/transport/validation code | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/scripts/validate_csv_against_reference.py` |
| `configs/event_4245_replay.yaml` | event 4245 replay, SSH, live NIC, and custom PCAP workflow config | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/configs/event_4245_replay.yaml` |
| `configs/event_4245_replay_ssh_template.yaml` | event 4245 replay, SSH, live NIC, and custom PCAP workflow config | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/configs/event_4245_replay_ssh_template.yaml` |
| `configs/live_nic_template.yaml` | event 4245 replay, SSH, live NIC, and custom PCAP workflow config | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/configs/live_nic_template.yaml` |
| `configs/live_nic_ssh_template.yaml` | event 4245 replay, SSH, live NIC, and custom PCAP workflow config | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/configs/live_nic_ssh_template.yaml` |
| `configs/custom_pcap_replay_template.yaml` | event 4245 replay, SSH, live NIC, and custom PCAP workflow config | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/configs/custom_pcap_replay_template.yaml` |
| `docs/fresh_setup.md` | setup, transport, architecture, live-data, and troubleshooting documentation | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/docs/fresh_setup.md` |
| `docs/external_jupyter_setup.md` | setup, transport, architecture, live-data, and troubleshooting documentation | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/docs/external_jupyter_setup.md` |
| `docs/standard_fabric_transport.md` | setup, transport, architecture, live-data, and troubleshooting documentation | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/docs/standard_fabric_transport.md` |
| `docs/architecture.md` | setup, transport, architecture, live-data, and troubleshooting documentation | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/docs/architecture.md` |
| `docs/live_data_mode.md` | setup, transport, architecture, live-data, and troubleshooting documentation | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/docs/live_data_mode.md` |
| `docs/adding_new_pcap.md` | setup, transport, architecture, live-data, and troubleshooting documentation | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/docs/adding_new_pcap.md` |
| `docs/troubleshooting.md` | setup, transport, architecture, live-data, and troubleshooting documentation | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/docs/troubleshooting.md` |
| `docs/dune_fabric_live_demo_README.md` | source package README for detailed package context | `/home/fabric/work/visualization-wibwiz/dune_fabric_live_demo/README.md` |
| `figures/event_4245_original_benchmark_heatmap.png` | original benchmark heatmap image | `/home/fabric/work/visualization-wibwiz/rebuild_restore/artifacts/final_presentation_evidence_2026-07-22/original_benchmark_4245/event_4245_original_benchmark_heatmap.png` |
| `figures/event_4245_original_benchmark_ridgeline.png` | original benchmark ridgeline image | `/home/fabric/work/visualization-wibwiz/rebuild_restore/artifacts/final_presentation_evidence_2026-07-22/original_benchmark_4245/event_4245_original_benchmark_ridgeline.png` |
| `figures/event_4245_live_demo_heatmap_latest.png` | latest polished live event 4245 heatmap preview from packaged replay pipeline | `/tmp/dune_fabric_live_demo/runs/presentation-live-4245-20260722T153315Z/visualizations/live_chunks/live_heatmap.png` |
| `figures/event_4245_live_demo_ridgeline_latest.png` | latest polished live event 4245 ridgeline preview from packaged replay pipeline | `/tmp/dune_fabric_live_demo/runs/presentation-live-4245-20260722T153315Z/visualizations/live_chunks/live_stacked_waveform.png` |
| `evidence/event_4245_chunk_wire_tick_coverage.png` | chunk wire/tick coverage plot explaining y-axis fill behavior | `/home/fabric/work/visualization-wibwiz/rebuild_restore/artifacts/final_presentation_evidence_2026-07-22/event_4245_chunk_wire_tick_coverage.png` |
| `evidence/initial_bottleneck_15_20_25pps_table.csv` | initial 15/20/25 pps bottleneck table | `/home/fabric/work/visualization-wibwiz/rebuild_restore/artifacts/final_presentation_evidence_2026-07-20/initial_bottleneck_15_20_25pps_table.csv` |
| `evidence/initial_bottleneck_evidence.md` | concise initial bottleneck explanation | `/home/fabric/work/visualization-wibwiz/rebuild_restore/reports/final_presentation_evidence_2026-07-20/initial_bottleneck_evidence.md` |
| `evidence/queue_v2_stress_matrix_15000_30000pps.csv` | Queue v2 15k/20k/30k pps compact stress-test table | `/home/fabric/work/visualization-wibwiz/rebuild_restore/artifacts/queue_v2_2026-07-14/event_6860_queue_v2_stress_matrix_15000_30000pps.csv` |
| `evidence/queue_v2_stress_matrix_15000_30000pps.md` | Queue v2 30,000 pps stress-test report | `/home/fabric/work/visualization-wibwiz/rebuild_restore/reports/queue_v2_2026-07-14/event_6860_queue_v2_stress_matrix_15000_30000pps.md` |
| `evidence/event_4245_live_visualization_timing.csv` | event 4245 live visualization timing/lag table | `/tmp/dune_fabric_live_demo/runs/presentation-live-4245-20260722T153315Z/reports/live_visualization_timing.csv` |
| `evidence/event_4245_live_run_summary.json` | small latest event 4245 live run summary | `/tmp/dune_fabric_live_demo/runs/presentation-live-4245-20260722T153315Z/reports/summary.json` |
| `evidence/event_4245_chunk_coverage_table.csv` | event 4245 per-chunk wire/tick coverage table | `/home/fabric/work/visualization-wibwiz/rebuild_restore/artifacts/final_presentation_evidence_2026-07-22/event_4245_chunk_coverage_table.csv` |
| `evidence/event_4245_chunk_coverage_analysis.md` | event 4245 chunk coverage explanation | `/home/fabric/work/visualization-wibwiz/rebuild_restore/reports/final_presentation_evidence_2026-07-22/event_4245_chunk_coverage_analysis.md` |
| `evidence/original_benchmark_4245.md` | original benchmark rendering provenance | `/home/fabric/work/visualization-wibwiz/rebuild_restore/reports/final_presentation_evidence_2026-07-22/original_benchmark_4245.md` |
| `evidence/event_4245_original_benchmark_render_summary.json` | original benchmark render summary metadata | `/home/fabric/work/visualization-wibwiz/rebuild_restore/artifacts/final_presentation_evidence_2026-07-22/original_benchmark_4245/event_4245_original_benchmark_render_summary.json` |
| `evidence/actual_replay_gif_ordering_check.md` | concise note that actual replay GIF was chunk-order based, not a left-to-right mask | `/home/fabric/work/visualization-wibwiz/rebuild_restore/reports/final_presentation_evidence_2026-07-20/actual_replay_gif_ordering_check.md` |

## Intentionally Omitted

| Source/pattern | Reason |
| --- | --- |
| `dune_fabric_live_demo/configs/event_6860_replay.yaml` | omitted to keep this artifact focused on event 4245 |
| `*.pcap` | large raw packet captures are not suitable for a minimal GitHub artifact |
| full decoded event CSVs / `combined.csv` | large runtime outputs are reproducible from PCAP replay and intentionally excluded |
| `/tmp/dune_fabric_live_demo/runs/*/raw_chunks/` | runtime-only raw chunks, not source artifact content |
| `__pycache__/`, `.ipynb_checkpoints/`, virtual environments | generated/cache content |
| large logs such as `commands.log`, notebook backend stdout/stderr | too verbose for minimal evidence; concise tables/reports are included instead |
