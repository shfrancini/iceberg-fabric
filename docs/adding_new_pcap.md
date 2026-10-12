# Adding A New PCAP

A different PCAP should require YAML edits only.

## Steps

1. Copy the baseline template:

```bash
cp configs/single_decoder_baseline_template.yaml configs/my_pcap.yaml
```

2. Set a unique `label` and optional `event_id`.

3. Set the sender PCAP and replay limit:

```yaml
pcap_replay:
  sender_pcap: /path/on/sender/input.pcap
  replay_timing: pps
  pps: 100
  packet_limit: 640
```

4. Set decode filters and receiver-side inputs:

```yaml
decode:
  tpc: 0
  plane: 2
paths:
  wibviz_src: /path/on/receiver/iceberg-fabric/wibviz_graph/src
  mapping_csv: /path/on/receiver/event-channel-map.csv
validation:
  enabled: true
  reference_csv: /path/on/receiver/reference.csv
```

5. Run the checks in order:

```bash
iceberg-fabric preflight --config configs/my_pcap.yaml --local-only
iceberg-fabric run --config configs/my_pcap.yaml --dry-run
iceberg-fabric preflight --config configs/my_pcap.yaml
iceberg-fabric run --config configs/my_pcap.yaml --max-live-chunks 2
```

6. Run the full replay only after the bounded smoke test passes:

```bash
iceberg-fabric run --config configs/my_pcap.yaml
```

No code changes are required unless the packet format is unsupported by the
existing WibViz decoder.
