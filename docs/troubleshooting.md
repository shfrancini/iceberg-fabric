# Troubleshooting

## Dry-Run First

Use dry-run before any replay or live capture:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --dry-run
```

Check the planned sender PCAP, receiver interface, capture filter, output root,
and validation reference.

## `tcpreplay` Not Found

`pcap_replay` requires `tcpreplay` on the sender VM. Install sender
dependencies before running replay mode.

## `tcpdump` Permission Or Interface Errors

The receiver must have `tcpdump`, sudo access, and the configured interface:

```bash
ip -brief addr show dev enp7s0
```

If the interface name differs, update `network.receiver_interface`.

## No Packets Captured

Check:

- the receiver interface name
- `network.capture_filter`
- sender and receiver data-plane IPs
- whether real live traffic is actually present for `live_nic`

The default capture filter is:

```text
udp and dst port 2049
```

## Decode Fails

The receiver must have the WIBViz source tree and channel mapping CSV:

```yaml
paths:
  wibviz_src: /home/ubuntu/wibviz_graph/src
  mapping_csv: /home/ubuntu/wibviz_inputs/event-4245-channel-map.csv
```

Decode logs are written under the run-specific receiver report directory.

## Validation Fails

Validation is exact CSV comparison over configured fields. Confirm that:

- the reference CSV matches the PCAP/event
- `compare_fields` are present in both generated and reference CSV files
- partial or live captures do not use a full-event reference

Disable validation for live data without a reference:

```bash
python3 scripts/run_pipeline.py --config configs/live_nic_template.yaml --no-validate
```

