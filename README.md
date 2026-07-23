# WIB/FABRIC Live Demo Artifact

WIB = Warm Interface Board.

This artifact demonstrates a FABRIC-based replay/capture/decode/validation/notebook visualization prototype for WIB detector packet data.

## What The System Does

The prototype supports two input modes:

- `pcap_replay`: a sender VM replays a staged PCAP into a receiver VM data-plane NIC.
- `live_nic`: the receiver VM captures packets already arriving on its data-plane NIC.

After packets reach the receiver, the shared pipeline:

- Captures receiver traffic into raw PCAP chunks.
- Preserves raw chunks for audit and debugging.
- Decodes chunks through a Queue v2 receiver-side path.
- Optionally validates decoded CSV output against a reference CSV.
- Copies compact outputs back to the Jupyter/local machine.
- Renders notebook heatmap and ridgeline views.
- Writes manifest, timing, queue, and validation summaries.

## Architecture

```text
pcap_replay mode: sender VM + tcpreplay + staged PCAP
                                             |
live_nic mode: packets already arriving on receiver NIC
                                             |
                                             v
receiver NIC -> raw PCAP chunks -> Queue v2 decode/export -> decoded CSV chunks
                                             |
                                             v
combined CSV -> validation summary -> notebook heatmap/ridgeline + timing CSV
```

The runner does not create FABRIC slices. It assumes sender/receiver VMs, VM-side files, and network interfaces already exist.

## Repository Layout

```text
notebooks/      Start-here and presentation notebooks
scripts/        Pipeline runner, transport abstraction, live viewer, validator
configs/        LoomAI and SSH YAML configs/templates
docs/           Setup, architecture, transport, live mode, troubleshooting notes
figures/        Included preview and benchmark figures
evidence/       Compact tables/reports from prior validation work
```

Open `notebooks/00_RUN_DEMO.ipynb` first. It is the front door for a new user.

## Computational Scripts

### `scripts/run_pipeline.py`

Main pipeline runner. It reads a YAML config, builds local and remote run directories, coordinates sender/receiver commands, captures raw chunks, calls decode/export on the receiver, copies local outputs, renders final visualizations, and writes manifest/timing/validation summaries.

### `scripts/remote_transport.py`

Remote execution and copy abstraction. It supports the LoomAI backend for the validated in-container path and an SSH backend for standard FABRIC or local Jupyter use.

### `scripts/live_chunk_viewer.py`

Notebook live viewer. It watches decoded chunk CSVs under a local run directory, updates cumulative heatmap and ridgeline plots, and records visualization timing data.

### `scripts/validate_csv_against_reference.py`

CSV validator. It compares a generated combined CSV against a reference CSV and reports missing rows, extra rows, and pass/fail status.

## Config Files

- `configs/event_4245_replay.yaml`: validated LoomAI event 4245 PCAP replay baseline.
- `configs/event_4245_replay_ssh_template.yaml`: external/local Jupyter SSH template for event 4245 replay; copy before editing.
- `configs/custom_pcap_replay_template.yaml`: LoomAI template for replaying a different staged PCAP.
- `configs/live_nic_template.yaml`: LoomAI template for bounded or intentional live NIC capture.
- `configs/live_nic_ssh_template.yaml`: SSH template for live NIC capture from standard FABRIC or local Jupyter.

Key path rule: `paths.output_root` is local to the Jupyter/runner machine. VM paths such as `paths.wibviz_src`, `paths.mapping_csv`, `validation.reference_csv`, and `pcap_replay.sender_pcap` must already exist on the configured VMs.

## Quick Start Inside FABRIC/LoomAI Jupyter

Run from the artifact root.

Dry-run only; this parses config and writes a local plan:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --dry-run
```

Two-chunk smoke test after the dry-run looks correct:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --max-live-chunks 2
```

Full event 4245 replay after the smoke test passes. The default event 4245 config is 640 packets, 64 packets per chunk, 10 chunks:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml
```

## External Jupyter / SSH Quick Start

The SSH path is dry-run verified only. A full external/local Jupyter smoke test still requires real VM hosts, a working key, and staged VM files.

Create or choose an SSH key:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/fabric_wibviz -C "fabric-wibviz"
cat ~/.ssh/fabric_wibviz.pub
```

Register or use the public key through the FABRIC/project access mechanism. Never share the private key.

Verify raw SSH to both VM management hosts:

```bash
ssh -i ~/.ssh/fabric_wibviz ubuntu@<sender-management-host> hostname
ssh -i ~/.ssh/fabric_wibviz ubuntu@<receiver-management-host> hostname
```

If needed, use a FABRIC SSH config or ProxyJump:

```bash
ssh -i ~/.ssh/fabric_wibviz -J <bastion-user>@<bastion-host> ubuntu@<sender-management-host> hostname
```

Copy and edit the replay SSH template:

```bash
cp configs/event_4245_replay_ssh_template.yaml configs/event_4245_replay_ssh.yaml
```

Fill in `transport.username`, `transport.sender_host`, `transport.receiver_host`, `transport.ssh_key`, optional `transport.ssh_options`, data-plane interfaces, and VM-side input paths.

Run external dry-run:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --dry-run
```

Run external two-chunk smoke test only after raw SSH, VM files, and dry-run all pass:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --max-live-chunks 2
```

## Expected Outputs

By default, local outputs are written under:

```text
/tmp/wib_fabric_live_demo/runs/<run_id>/
```

Important outputs:

- `reports/manifest.json`: status, paths, chunks, queue metrics, validation result, pass/fail.
- `reports/summary.json`: compact run summary when written by the runner.
- `reports/live_visualization_timing.csv`: live viewer timing/lag table when the notebook viewer is used.
- `artifacts/decoded_chunks/chunk_*.csv`: decoded chunk CSVs copied locally.
- `artifacts/combined.csv`: final combined decoded CSV.
- `visualizations/heatmap.png`: final heatmap.
- `visualizations/stacked_waveform.png`: final ridgeline-style waveform view.
- `visualizations/live_chunks/`: cumulative live display images when the viewer is used.

Remote receiver products are written under `<receiver_root>/wibviz_runs/wib_fabric_live_demo/<run_id>/` and `<receiver_root>/wibviz_outputs/wib_fabric_live_demo/<run_id>/`.

## Evidence Included

The artifact includes compact evidence, not large runtime products:

- `figures/`: included heatmap/ridgeline preview and benchmark images.
- `evidence/event_4245_live_visualization_timing.csv`: timing/lag table from a prior local presentation run.
- `evidence/event_4245_chunk_coverage_table.csv`: chunk coverage table.
- `evidence/event_4245_chunk_coverage_analysis.md`: explanation of wire-band fill behavior.
- `evidence/queue_v2_stress_matrix_15000_30000pps.csv`: compact Queue v2 stress summary.

Large PCAPs, full decoded event CSVs, raw chunks, verbose logs, caches, checkpoints, and virtual environments are intentionally omitted.

## Limitations And Future Work

- External/local Jupyter is dry-run verified only until tested with real SSH host/key details and staged VM-side files.
- Real live NIC mode requires a prepared live packet source feeding the receiver NIC.
- File-based CSV handoff supports reproducibility and auditability but adds latency.
- Adaptive chunking is experimental/future-facing and disabled by default.
- Slice creation/provisioning, VM dependency installation, host discovery, and input-file staging remain manual.

## Artifact / Repo Publication Note

For publication, use a neutral repository or folder name such as `wib-fabric-live-demo`. Keep this README, `notebooks/00_RUN_DEMO.ipynb`, and the SSH templates aligned if paths or default config names change.
