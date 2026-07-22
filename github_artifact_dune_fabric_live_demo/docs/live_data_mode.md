# Live Data Mode

`live_nic` is the real-data direction. It listens on the receiver NIC for packets that are already arriving from the detector/data path. It does not start `tcpreplay`, does not require a sender VM, and does not assume a fixed total packet count.

## PCAP Replay vs Live NIC

PCAP replay mode is the controlled test mode. It uses `tcpreplay` on the sender VM to inject a known PCAP into the receiver path.

Live NIC mode skips `tcpreplay`. The receiver captures real incoming packets directly from `network.receiver_interface`.

After packets reach the receiver, both modes should use the same Queue v2 path:

```text
receiver NIC -> raw PCAP chunks -> queue -> decoded CSV chunks -> manifest/status -> visualization
```

## Capture Model

The receiver captures rolling time-window chunks:

```yaml
live_nic:
  chunk_seconds: 5
  max_chunks: null
```

Each chunk is written as a raw PCAP, queued, decoded, appended to the combined CSV, and reflected in the manifest.

## Required Receiver State

The receiver VM must already have:

- live traffic arriving on `network.receiver_interface`
- `tcpdump` and non-interactive `sudo tcpdump`
- `python3`
- WIBViz source at `paths.wibviz_src`
- mapping CSV at `paths.mapping_csv`

## Bounded vs Unbounded Runs

For safety, the runner requires either:

- `--max-live-chunks N`, or
- `--follow` for an unbounded run that stops when `live_nic.stop_file` appears.

Examples:

```bash
python3 scripts/run_pipeline.py --config configs/live_nic_template.yaml --max-live-chunks 3
python3 scripts/run_pipeline.py --config configs/live_nic_template.yaml --follow
```

For SSH transport, copy and edit the SSH template first:

```bash
cp configs/live_nic_ssh_template.yaml configs/live_nic_ssh.yaml
python3 scripts/run_pipeline.py --config configs/live_nic_ssh.yaml --dry-run
python3 scripts/run_pipeline.py --config configs/live_nic_ssh.yaml --max-live-chunks 3
```

To stop a `--follow` run, create the stop file on the receiver VM:

```bash
touch /tmp/dune_fabric_live_demo.stop
```

## Validation

Validation is disabled in the live template. Real data often has no complete reference CSV. If a live feed has event boundaries and a reference is available, enable `validation.enabled` and set `validation.reference_csv` to a receiver VM path.

## Current Limitation

Live continuous streaming is a prototype workflow, not a polished production service. Use bounded chunks first and inspect `reports/manifest.json` before attempting `--follow`.
