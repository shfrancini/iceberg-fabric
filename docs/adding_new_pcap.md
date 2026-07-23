# Adding A New PCAP

You can replay a different PCAP without changing Python code. The runner reads all event-specific choices from YAML.

## Steps

1. Copy the template:

```bash
cp configs/custom_pcap_replay_template.yaml configs/my_new_pcap.yaml
```

2. Set a unique `label` and, if useful, `event_id`.

3. Set `pcap_replay.sender_pcap` to the absolute PCAP path on the sender VM:

```yaml
pcap_replay:
  sender_pcap: /home/ubuntu/wib-replay/pcap/my-event.pcap
```

4. Choose replay timing:

```yaml
pcap_replay:
  replay_timing: pps
  pps: 50
```

Use `replay_timing: original` to let `tcpreplay` use the PCAP timing.

5. Choose decode filters:

```yaml
decode:
  tpc: 0
  plane: all
```

6. Confirm receiver-side files:

```yaml
paths:
  wibviz_src: /home/ubuntu/wibviz_graph/src
  mapping_csv: /home/ubuntu/wibviz_inputs/event-4245-channel-map.csv
```

7. Leave validation disabled unless you have a matching reference CSV on the receiver VM:

```yaml
validation:
  enabled: true
  reference_csv: /home/ubuntu/wibviz_inputs/my-reference.csv
  compare_fields: seq_id,timestamp,tick,adc_value,wire_id,tpc,slot,plane
```

8. Dry-run first:

```bash
python3 scripts/run_pipeline.py --config configs/my_new_pcap.yaml --dry-run
```

9. Run a bounded smoke test before a full replay:

```bash
python3 scripts/run_pipeline.py --config configs/my_new_pcap.yaml --max-live-chunks 2
```

10. Run only after the dry-run and smoke test point to the expected sender PCAP, interfaces, receiver paths, and validation behavior:

```bash
python3 scripts/run_pipeline.py --config configs/my_new_pcap.yaml
```

## Notes

- The sender VM must already contain the PCAP and `tcpreplay`.
- The receiver VM must already contain WIBViz, the mapping CSV, and any reference CSV.
- For external/local Jupyter, start from `configs/event_4245_replay_ssh_template.yaml` and change `pcap_replay.sender_pcap` plus the same event-specific fields.
- No code changes are required unless the PCAP packet format is unsupported by the existing WIBViz decoder.
