# DUNE FABRIC Live Demo Artifact

WIB = Warm Interface Board.

Minimal GitHub-ready artifact for the DUNE WIBViz FABRIC live visualization pipeline. The main reproducible path is the event 4245 PCAP replay demo: Queue v2 capture, raw chunk preservation, decode/export, validation, and notebook visualization with benchmark-style full-range heatmap plus ridgeline views.

Repository URL placeholder: https://github.com/<username>/<repo-name>

## What Is Included

- `notebooks/00_RUN_DEMO.ipynb`: Start Here notebook. Open this first.
- `notebooks/event_4245_live_demo_presentation.ipynb`: focused event 4245 live presentation notebook.
- `scripts/`: runner, live chunk viewer, transport abstraction, and validation helper.
- `configs/`: event 4245 replay config, SSH template config, live NIC templates, and custom PCAP template.
- `docs/`: setup, external Jupyter, transport, architecture, live data, new PCAP, and troubleshooting notes.
- `figures/`: benchmark and latest live preview images.
- `evidence/`: small tables/reports documenting bottleneck, Queue v2 stress results, event 4245 timing, and chunk coverage.

Large PCAPs, full decoded event CSV outputs, run logs, caches, virtual environments, and VM-local runtime directories are intentionally not included.

## Quick Start Inside FABRIC/LoomAI Jupyter

From the artifact root:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --dry-run
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --max-live-chunks 2
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml
```

For notebook use, open `notebooks/00_RUN_DEMO.ipynb` first. It explains the operating model, VM prerequisites, dry-run, smoke test, full replay, and live display. Then use `notebooks/event_4245_live_demo_presentation.ipynb` for the focused event 4245 presentation path.

## Quick Start From External Or Local Jupyter Using SSH

1. Confirm or create an SSH key:

```bash
ls -la ~/.ssh
ssh-keygen -t ed25519 -f ~/.ssh/fabric_wibviz -C "fabric-wibviz"
cat ~/.ssh/fabric_wibviz.pub
```

Register the public key with the FABRIC/project access mechanism. Never share the private key.

2. Verify raw SSH access to both VMs:

```bash
ssh -i ~/.ssh/fabric_wibviz ubuntu@<sender-management-host> hostname
ssh -i ~/.ssh/fabric_wibviz ubuntu@<receiver-management-host> hostname
```

If a bastion is required, use `-J <bastion-user>@<bastion-host>` or a FABRIC-generated SSH config with `-F /path/to/fabric_ssh_config`.

3. Copy and edit the SSH template:

```bash
cp configs/event_4245_replay_ssh_template.yaml configs/event_4245_replay_ssh.yaml
```

Fill in:

- `transport.username`
- `transport.sender_host`
- `transport.receiver_host`
- `transport.ssh_key`
- optional `transport.ssh_options` for `ProxyJump` or `-F /path/to/fabric_ssh_config`
- `network.sender_interface` and `network.receiver_interface`
- `pcap_replay.sender_pcap`
- `paths.wibviz_src`
- `paths.mapping_csv`
- `validation.reference_csv` if validation is enabled

4. Run package-level SSH dry-run:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --dry-run
```

5. Run the bounded SSH smoke test only after raw SSH and dry-run are correct:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --max-live-chunks 2
```

6. Run the full event 4245 replay:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml
```

## Required Files On FABRIC VMs

Sender VM:

- `tcpreplay` installed.
- Event 4245 PCAP staged at the path in `pcap_replay.sender_pcap`.
- Correct data-plane interface configured in `network.sender_interface`.

Receiver VM:

- `tcpdump` installed.
- WIBViz source tree staged at `paths.wibviz_src`.
- Event/channel mapping CSV staged at `paths.mapping_csv`.
- Optional validation reference CSV staged at `validation.reference_csv`.
- Correct data-plane interface configured in `network.receiver_interface`.

Local/Jupyter machine:

- Python 3 with `pyyaml`, `pandas`, and `matplotlib`.
- `ssh` and `scp`; `rsync` only if selected in config.

## Outputs

By default, runtime outputs go under:

```text
/tmp/dune_fabric_live_demo/runs/<run_id>/
```

Important subdirectories/files:

- `raw_chunks/`: preserved raw PCAP chunks on receiver-side runtime path.
- `artifacts/decoded_chunks/chunk_*.csv`: decoded CSV chunk outputs.
- `artifacts/combined.csv`: final combined decoded CSV.
- `reports/manifest.json`: run status, validation, and queue metrics.
- `reports/live_visualization_timing.csv`: live display timing/lag table when the notebook live viewer is used.
- `visualizations/live_chunks/live_heatmap.png`: cumulative live heatmap preview.
- `visualizations/live_chunks/live_stacked_waveform.png`: cumulative live ridgeline preview.

## Known Limitations

- This artifact does not include PCAPs or full decoded event CSVs.
- The LoomAI transport path is the most validated path in the current working system.
- SSH transport is implemented and dry-run validated; a real external SSH smoke test requires reachable VMs, correct keys, and staged VM-side files.
- The runner does not create or provision FABRIC slices. It assumes sender/receiver VMs already exist.
- Evidence stress tables are compact and intentionally omit large raw logs and full runtime products.

## Real Live Data vs PCAP Replay

PCAP replay is the controlled reproducibility mode. It sends a known event 4245 PCAP through the same receiver-side Queue v2 pipeline used by live NIC capture.

Live NIC mode is the real-data direction. In live NIC mode, traffic is assumed to arrive on the receiver NIC from an external source. Use bounded `--max-live-chunks` tests first. Unbounded `--follow` capture should be used intentionally and monitored.

In both modes, chunks are packet batches, not tick/time image slices. The event 4245 live visualization can fill by wire/y-axis regions because each packet batch may cover the full tick range while only covering part of the wire range.
