# Live NIC Mode

`live_nic` captures packets already arriving on the receiver interface. It does
not run `tcpreplay` and is not the default public baseline workflow.

## Minimal Shape

```yaml
source_mode: live_nic
network:
  receiver_interface: enp7s0
  capture_filter: udp and dst port 2049
live_nic:
  chunk_seconds: 5
  max_chunks: 3
```

The receiver still needs `python3`, `tcpdump`, capture permission, WibViz source,
and a mapping CSV.

## Safe Usage

Run bounded captures first:

```bash
iceberg-fabric run --config configs/my_live_nic.yaml --max-live-chunks 3
```

Only use `--follow` intentionally. An unbounded live run stops when the
configured receiver-side stop file appears.

## Validation

Live captures often do not have a full exact reference. Leave validation disabled
unless you have a reference CSV matching the captured window and decode filters.
