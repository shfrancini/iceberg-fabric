# Architecture

This package is one system with two input adapters.

```text
pcap_replay adapter: sender VM tcpreplay selected PCAP
                                             |
live_nic adapter: real traffic already on receiver NIC
                                             |
                                             v
receiver NIC -> raw chunk queue -> decode/export -> manifest/status
             -> visualization -> optional validation
```

## Input Modes

`pcap_replay` is for repeatable demos, validation, and stress tests. The sender
VM uses `tcpreplay` to inject the configured PCAP into the experiment interface.
The receiver captures the resulting packets into raw chunk PCAPs before decode.

`live_nic` does not use `tcpreplay` or a sender PCAP. It assumes traffic is
already arriving on the receiver interface. The receiver captures rolling
time-window chunks and can run without a packet-count limit.

## Shared Pipeline Boundary

The architectural boundary is the receiver NIC. Once packets reach that NIC, the
same downstream contract applies to both modes:

- raw chunk PCAPs are preserved first
- raw chunks enter the queue
- `wibviz.export` decodes each raw chunk into CSV
- decoded chunks are appended into a combined CSV
- `manifest.json` reports queue, decode, output, and validation status
- heatmap and stacked waveform rendering reads the combined CSV
- validation runs only when a reference CSV is configured

## Defaults

The default notebook selection is event 4245 in `pcap_replay` mode with fixed
64-packet chunks, Queue v2 semantics, heatmap output, and stacked waveform
output.

Event 6860 is the validation/performance demo. It uses the same runner and can
be run with fixed 64-packet chunks or with experimental adaptive mode enabled.

## Runtime Locations

Local outputs default to:

```text
/tmp/dune_fabric_live_demo/runs/<run_id>/
```

Receiver VM outputs default to:

```text
/home/ubuntu/wibviz_runs/dune_fabric_live_demo/<run_id>/
/home/ubuntu/wibviz_outputs/dune_fabric_live_demo/<run_id>/
```

