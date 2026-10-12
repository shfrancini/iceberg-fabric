# External Jupyter Setup

Use SSH transport when running from a laptop, workstation, or standard FABRIC
notebook instead of LoomAI.

## Install

```bash
python3 -m pip install -e '.[dev,dashboard,notebook]'
```

## Configure SSH Transport

Copy the baseline config:

```bash
cp configs/single_decoder_baseline_template.yaml configs/my_baseline.yaml
```

Set SSH details:

```yaml
transport:
  type: ssh
  username: ubuntu
  ssh_key: ~/.ssh/fabric_slice_key
  ssh_options:
    - -o
    - BatchMode=yes
    - -o
    - StrictHostKeyChecking=accept-new
  copy_tool: scp
  hosts:
    sender: sender.example.fabric.testbed.net
    receiver: receiver.example.fabric.testbed.net
slice:
  name: your-fabric-slice
  sender_node: sender
  receiver_node: receiver
```

If FABRIC access requires a generated SSH config or jump host, add it to
`transport.ssh_options`.

## Configure Data And Paths

Set example paths to real locations in your environment:

```yaml
network:
  sender_interface: enp7s0
  receiver_interface: enp7s0
paths:
  output_root: /tmp/iceberg_fabric_runs
  receiver_root: /tmp/iceberg_fabric_receiver
  sender_root: /tmp/iceberg_fabric_sender
  wibviz_src: /path/on/receiver/iceberg-fabric/wibviz_graph/src
  mapping_csv: /path/on/receiver/event-channel-map.csv
pcap_replay:
  sender_pcap: /path/on/sender/input.pcap
validation:
  reference_csv: /path/on/receiver/reference.csv
```

## Run Order

```bash
iceberg-fabric preflight --config configs/my_baseline.yaml --local-only
iceberg-fabric run --config configs/my_baseline.yaml --dry-run
iceberg-fabric preflight --config configs/my_baseline.yaml
iceberg-fabric run --config configs/my_baseline.yaml --max-live-chunks 2
iceberg-fabric run --config configs/my_baseline.yaml
```

Do not run the final command until the bounded smoke test passes and the paths,
interfaces, and validation reference are confirmed.
