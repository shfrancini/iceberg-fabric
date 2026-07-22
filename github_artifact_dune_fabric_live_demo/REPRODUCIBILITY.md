# Reproducibility

Run commands from the artifact root.

## 1. Install Local Dependencies

```bash
python3 -m pip install pyyaml pandas matplotlib
```

Confirm local tools:

```bash
ssh -V
scp -V 2>/dev/null || true
```

## 2. LoomAI / FABRIC Jupyter Path

Dry-run:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --dry-run
```

Two-chunk smoke test:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --max-live-chunks 2
```

Full event 4245 replay:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml
```

## 3. External / Standard Jupyter SSH Path

Copy the SSH template:

```bash
cp configs/event_4245_replay_ssh_template.yaml configs/event_4245_replay_ssh.yaml
```

Edit the copied config with real VM management hosts, username, SSH key, optional ProxyJump or FABRIC SSH config, data-plane interfaces, and remote file paths.

SSH dry-run:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --dry-run
```

SSH smoke test:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --max-live-chunks 2
```

SSH full replay:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml
```

## 4. Notebook Order

1. `notebooks/00_RUN_DEMO.ipynb`
2. `notebooks/event_4245_live_demo_presentation.ipynb`

Both notebooks in this artifact are output-stripped. Running them will create local outputs under `/tmp/dune_fabric_live_demo/runs/<run_id>/` unless configured otherwise.

## 5. VM-Side Prerequisites

Sender VM must have `tcpreplay` and the event 4245 PCAP. Receiver VM must have `tcpdump`, WIBViz source, mapping CSV, and optional validation reference CSV. See `docs/external_jupyter_setup.md` and `docs/fresh_setup.md` for the full checklist.

## 6. Evidence Without Rerun

The `figures/` and `evidence/` directories contain copied, compact evidence from the current working system. These are for understanding and verification; they are not required to run the replay pipeline.
