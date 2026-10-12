# Troubleshooting

## Start With Preflight

Run a local-only check before testing credentials:

```bash
iceberg-fabric preflight --config configs/single_decoder_baseline_template.yaml --local-only
```

Then run against your real config:

```bash
iceberg-fabric preflight --config configs/my_baseline.yaml
```

Every `FAIL` line includes a repair instruction. Fix required failures before
running any replay.

## Dependency Failures

If preflight reports a missing Python dependency, reinstall from the clone root:

```bash
python3 -m pip install -e '.[dev,dashboard,notebook]'
```

If optional dashboard or notebook dependencies are missing and you do not use
those workflows, the warning can be ignored.

## SSH Or LoomAI Command Missing

For SSH transport, install or expose `ssh` and `scp` locally and set:

```yaml
transport:
  type: ssh
  username: ubuntu
  ssh_key: ~/.ssh/fabric_slice_key
  hosts:
    sender: sender.example.fabric.testbed.net
    receiver: receiver.example.fabric.testbed.net
```

For LoomAI transport, run inside an environment where `loomai` is installed and
configured, then use `transport.type: loom`.

## Sender Or Receiver Unreachable

Check the configured node names and hosts. For SSH transport, `slice.sender_node`
and `slice.receiver_node` must match keys in `transport.hosts`. Add SSH config
or ProxyJump options through `transport.ssh_options` when your FABRIC access
requires a bastion.

## Remote Tool Failures

Replay mode requires `tcpreplay` on the sender and `tcpdump` on the receiver.
The receiver must also be able to run `sudo -n tcpdump` non-interactively.
Install missing VM packages or update sudo policy before replay.

## Interface Or Capture Permission Failures

Set the data-plane interface names, not management interfaces:

```yaml
network:
  sender_interface: enp7s0
  receiver_interface: enp7s0
  capture_filter: udp and dst port 2049
```

If `tcpdump` captures no packets, verify the interface, filter, sender replay
interface, and data-plane routing outside this runner.

## PCAP, Mapping, Reference, Or WibViz Path Failures

The runner does not copy large inputs for you. Confirm these paths exist on the
correct VM:

```yaml
paths:
  wibviz_src: /path/on/receiver/iceberg-fabric/wibviz_graph/src
  mapping_csv: /path/on/receiver/event-channel-map.csv
pcap_replay:
  sender_pcap: /path/on/sender/input.pcap
validation:
  reference_csv: /path/on/receiver/reference.csv
```

Disable validation only when you intentionally do not have a matching reference:

```bash
iceberg-fabric run --config configs/my_baseline.yaml --no-validate --dry-run
```

## Validation Fails

Exact validation compares configured fields row by row. Use a reference CSV from
the same PCAP, mapping, TPC/plane filters, packet limit, and packet ordering.
Partial smoke tests should use matching partial references or `--no-validate`.

## Disk Space Or Output Permission Failures

Set `paths.output_root` to a local writable filesystem with enough space for
reports, copied decoded chunks, and visualizations. Set `paths.receiver_root` to
a writable receiver VM directory and confirm it has enough space for raw chunks
and temporary decode files.
