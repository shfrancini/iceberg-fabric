# Fresh User Checklist

Use this checklist before running `wib_fabric_live_demo` on a new FABRIC slice or from external/local Jupyter. The first file to open is `00_RUN_DEMO.ipynb`.

## Current Status

- LoomAI transport is currently validated.
- SSH transport is implemented and dry-run validated.
- A real SSH smoke test still requires actual VM host/key details, reachable VMs, and VM-side files.

## 1. Choose The Transport

Use LoomAI when running inside the validated LoomAI Jupyter environment:

```yaml
transport:
  type: loom
```

Use SSH when running from standard FABRIC or local Jupyter:

```yaml
transport:
  type: ssh
  username: ubuntu
  sender_host: <sender-management-ip-or-dns-or-ssh-config-alias>
  receiver_host: <receiver-management-ip-or-dns-or-ssh-config-alias>
  ssh_key: ~/.ssh/fabric_slice_key
```

For SSH, copy a template first:

```bash
cp configs/event_4245_replay_ssh_template.yaml configs/event_4245_replay_ssh.yaml
cp configs/live_nic_ssh_template.yaml configs/live_nic_ssh.yaml
```

If your access goes through FABRIC bastions, add the generated SSH config or ProxyJump options in `transport.ssh_options`:

```yaml
ssh_options:
  - -F
  - /path/to/fabric_ssh_config
  - -o
  - BatchMode=yes
```

or:

```yaml
ssh_options:
  - -J
  - bastion.example.org
  - -o
  - BatchMode=yes
```

## 2. What Must Already Exist

Before real smoke/full runs, confirm:

- active FABRIC slice
- sender VM reachable for `pcap_replay`
- receiver VM reachable
- detector/data interface name known
- `tcpdump` installed on receiver
- `tcpreplay` installed on sender for replay mode
- Python dependencies available on receiver for WIBViz decode/export
- PCAP file on sender for replay mode
- mapping CSV on receiver
- optional reference CSV on receiver for validation
- SSH credentials/config if using external/local Jupyter

## 3. Confirm VM Files And Tools

Receiver VM requirements:

- `python3`
- `tcpdump`
- passwordless or non-interactive `sudo tcpdump`
- WIBViz source tree at `paths.wibviz_src`, for example `/home/ubuntu/wibviz_graph/src`
- channel mapping CSV at `paths.mapping_csv`
- reference CSV if `validation.enabled: true`

Sender VM requirements for `pcap_replay`:

- `tcpreplay`
- source PCAP at `pcap_replay.sender_pcap`
- configured sender data-plane interface at `network.sender_interface`

The runner uploads only its helper validator script. It does not install packages, clone WIBViz, create the slice, discover IPs, or upload input PCAP/reference files.

## 4. Configure PCAP Replay Mode

Start from one of these:

```bash
configs/event_4245_replay.yaml
configs/event_6860_replay.yaml
configs/custom_pcap_replay_template.yaml
configs/event_4245_replay_ssh_template.yaml
```

Key fields:

```yaml
source_mode: pcap_replay
network:
  sender_interface: enp7s0
  receiver_interface: enp7s0
  capture_filter: udp and dst port 2049
paths:
  output_root: /tmp/wib_fabric_live_demo   # local to the Jupyter machine
  wibviz_src: /home/ubuntu/wibviz_graph/src # receiver VM path
  mapping_csv: /home/ubuntu/wibviz_inputs/event-4245-channel-map.csv
pcap_replay:
  sender_pcap: /home/ubuntu/wib-replay/pcap/my-event.pcap
  replay_timing: pps
  pps: 50
  packet_limit: 640
```

Use `packet_limit` for a bounded event replay. Use CLI `--max-live-chunks 2` for a smoke test without editing the config.

## 5. Configure Live NIC Mode

Start from one of these:

```bash
configs/live_nic_template.yaml
configs/live_nic_ssh_template.yaml
```

Key fields:

```yaml
source_mode: live_nic
network:
  receiver_interface: enp7s0
  capture_filter: udp and dst port 2049
live_nic:
  chunk_seconds: 5
  max_chunks: null
  stop_file: /tmp/wib_fabric_live_demo.stop
```

Live mode does not start `tcpreplay` and does not require a sender. For safety, run bounded captures with `--max-live-chunks N` unless you intentionally use `--follow`.

## 6. Run In Order

Dry-run first:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --dry-run
```

Bounded LoomAI smoke test:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --max-live-chunks 2
```

Full LoomAI replay:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml
```

SSH dry-run after editing copied template:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --dry-run
```

SSH smoke test after real host/key/path details are confirmed:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --max-live-chunks 2
```

## Custom PCAP Without Code Changes

1. Copy the template:

```bash
cp configs/custom_pcap_replay_template.yaml configs/my_pcap.yaml
```

2. Edit only YAML fields: `label`, `event_id`, `pcap_replay.sender_pcap`, `decode.tpc`, `decode.plane`, `paths.mapping_csv`, and optional `validation` fields.

3. Dry-run:

```bash
python3 scripts/run_pipeline.py --config configs/my_pcap.yaml --dry-run
```

4. Smoke test:

```bash
python3 scripts/run_pipeline.py --config configs/my_pcap.yaml --max-live-chunks 2
```

No Python code changes are required for a different PCAP as long as the packet format is still supported by the existing WIBViz decoder.

## Still Not Automatic

- Creating or provisioning the FABRIC slice.
- Discovering sender/receiver management hosts for SSH configs.
- Installing `tcpdump`, `tcpreplay`, Python dependencies, or WIBViz on VMs.
- Copying source PCAPs, mapping CSVs, or reference CSVs to VMs.
- Determining the correct data-plane interface names for a new topology.
- Configuring FABRIC bastion/ProxyJump SSH access from external/local Jupyter.
- Operating live continuous stream as a polished production service.
