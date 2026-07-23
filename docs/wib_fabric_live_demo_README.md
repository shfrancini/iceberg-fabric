# WIB/FABRIC Live Demo

Packaged implementation guide and runner for the WIBViz FABRIC receiver visualization pipeline. The main entry point is `00_RUN_DEMO.ipynb`; it now walks a new user through the full operating model, required VM state, execution mode selection, dry-run planning, bounded smoke tests, full replay, pass/fail metrics, and visualization display.

The package has one runner, `scripts/run_pipeline.py`. The input mode changes how packets arrive at the receiver NIC; the downstream Queue v2 raw chunk, decode/export, manifest, visualization, and optional validation stages are shared.

## Current Validation Status

- LoomAI transport is currently validated with event 4245 in `pcap_replay` mode.
- SSH transport is implemented and dry-run validated.
- A real external/local Jupyter SSH smoke test still requires actual FABRIC VM host/key details and prepared VM-side files.

Validated LoomAI event 4245 full run:

- full run ID: `20260716T185535Z-event-4245-pcap_replay`
- 10 raw chunks preserved
- 409,600 decoded rows
- validation passed with 0 missing / 0 extra
- heatmap and stacked waveform generated

## Start Here

1. Open `00_RUN_DEMO.ipynb` first.
2. Use `transport_backend = "loom"` for the currently validated LoomAI path.
3. Use `transport_backend = "ssh"` for the external/local Jupyter portability path after copying and editing an SSH config template.
4. Run dry-run before any smoke test or full replay.
5. Run a bounded 2-chunk smoke test before full replay.

## Architecture

```text
pcap_replay mode: sender VM tcpreplay selected PCAP
                                             |
live_nic mode: real traffic already on receiver NIC
                                             |
                                             v
receiver NIC -> raw PCAP chunks -> queue -> decoded CSV chunks -> manifest/status -> visualization
```

PCAP replay is the controlled test mode. Live NIC mode is the real-data direction. After packets reach the receiver, both modes use the same Queue v2 receiver-side path.

## Live Chunk-By-Chunk Notebook Display

The standard packaged run still produces final outputs after completion: `artifacts/combined.csv`, `visualizations/heatmap.png`, and `visualizations/stacked_waveform.png` under `/tmp/wib_fabric_live_demo/runs/<run_id>/`.

`00_RUN_DEMO.ipynb` also includes a **Live Chunk-by-Chunk Demo** section. That notebook path creates a unique `RUN_ID`, starts a stable IPython display watcher, runs `scripts/run_pipeline.py` in a background subprocess, and updates cumulative heatmap plus stacked waveform as decoded chunk CSVs appear under `artifacts/decoded_chunks/chunk_*.csv`.

Default live demo settings are event 4245, pcap replay mode, fixed 64-packet chunks, and `live_demo_mode = "full"`, which runs 640 packets as 10 chunks. Set `live_demo_mode = "smoke"` for the 2-chunk fallback connectivity test.

## CLI Examples

LoomAI dry-run:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --dry-run
```

LoomAI bounded smoke test:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --max-live-chunks 2
```

LoomAI full replay:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml
```

SSH template dry-run after copying and editing host/key fields:

```bash
cp configs/event_4245_replay_ssh_template.yaml configs/event_4245_replay_ssh.yaml
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --dry-run
```

Live NIC bounded capture:

```bash
python3 scripts/run_pipeline.py --config configs/live_nic_template.yaml --max-live-chunks 3
```

## Minimum External-Jupyter Path

1. Copy this package to the local machine or clone the repository.
2. Install local Python dependencies used by the runner/notebook: `pyyaml`, `pandas`, and `matplotlib`.
3. Ensure local `ssh` plus `scp` or `rsync` are available.
4. Copy an SSH template and fill in host/key/path fields.
5. Run dry-run.
6. Run a 2-chunk smoke test only after VM reachability and VM-side files are confirmed.
7. Inspect local outputs under `/tmp/wib_fabric_live_demo/runs/<run_id>/` unless `--output-root` is set.

## Transport Backends

Remote access is selected with `transport.type`:

- `loom` preserves the currently validated LoomAI behavior and uses `loomai exec` / `loomai scp` internally.
- `ssh` uses standard `ssh` plus `scp` or `rsync` for standard FABRIC/local Jupyter use.

The pipeline logic calls only the transport interface. Backend-specific command execution is isolated in `scripts/remote_transport.py`.

## Docs

- `00_RUN_DEMO.ipynb`: first file to open; end-to-end implementation guide and runnable notebook.
- `docs/fresh_setup.md`: checklist for VM files, tools, configs, transport, and what remains manual.
- `docs/external_jupyter_setup.md`: minimum external/local Jupyter SSH setup path.
- `docs/standard_fabric_transport.md`: LoomAI vs SSH transport details.
- `docs/adding_new_pcap.md`: how to swap in a different PCAP without code changes.
- `docs/live_data_mode.md`: bounded and unbounded live NIC capture behavior.
- `docs/troubleshooting.md`: common failures and where to look.

Runtime outputs default to `/tmp/wib_fabric_live_demo`. Avoid writing large runtime outputs into `/home/fabric/work`.
