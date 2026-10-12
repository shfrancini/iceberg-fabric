# Architecture

The public baseline is a finite replay-to-visualization pipeline for an existing
FABRIC slice. It does not create the slice and it does not deploy VM software.

```text
sender VM
  user PCAP -> tcpreplay on sender interface
                         |
                         v
receiver VM
  tcpdump on receiver interface -> 64-packet raw chunks
                         |
                         v
local controller
  bounded raw queue, capacity 4
                         |
                         v
receiver VM
  one persistent WibViz decoder process, batch size 1 by default
                         |
                         v
local controller
  ordered append -> exact CSV validation -> PNG visualization -> reports
```

## Stable Baseline

The stable public defaults are:

- 64-packet chunks.
- Concurrent capture, decode, and ordered output.
- Bounded raw queue with capacity 4.
- One decoder controller.
- One persistent receiver-side decoder.
- `persistent_batch_chunks: 1` by default.
- Ordered append output.
- Exact post-output validation when a matching reference CSV is configured.
- Post-run heatmap and stacked waveform rendering.
- Queue, latency, throughput, batch, and backpressure metrics.

Multi-chunk batching is a configurable experiment through
`decode.queue.persistent_batch_chunks`, but it remains a single-decoder path.

## Input Modes

`pcap_replay` is the public baseline mode. It sends a user-provided PCAP from a
sender VM with `tcpreplay` and captures the packets on the receiver VM.

`live_nic` is available for bounded receiver-side captures when traffic is
already arriving on the receiver interface. It is not the primary public replay
workflow and should be run with explicit bounds first.

## Transport Boundary

The pipeline uses a small transport abstraction with `exec`, `copy_to`, and
`copy_from` operations. `transport.type: loom` uses the LoomAI CLI. `transport.type: ssh`
uses standard SSH/SCP and a host map in the YAML config.

The transport does not discover topology, install packages, upload PCAPs, or
change FABRIC slice state.

## Output Boundary

The local run directory contains copied decoded artifacts, reports, validation
results, and visualizations. Receiver-side temporary directories are created
under `paths.receiver_root`; sender-side temporary directories are created under
`paths.sender_root` for replay mode.
