# FABRIC event-4245 live replay demonstration

This runbook reproduces the validated DUNE WIB packet demonstration on the
FABRIC slice `dune-udp-demo-0ba777`.

## Validated result

- 640 packets replayed from the sender VM.
- 640 packets captured and parsed on the receiver VM.
- 0 packets skipped.
- 409,600 graph-ready detector rows produced.
- 409,601 lines present at both the receiver and LoomAI viewer.
- Every detector row matched the known event-4245 reference CSV.

The source event is prerecorded. Packet transmission, capture, decoding,
mapping, CSV growth, snapshot transfer, and visualization occur while the
demonstration is running.

## Topology and terminal roles

| Terminal | Prompt | Role |
|---|---|---|
| Sender VM | `ubuntu@sender` | Replays the PCAP on `enp7s0` |
| Receiver VM | `ubuntu@receiver` | Captures, parses, maps, and snapshots rows |
| LoomAI | `fabric@loomai-...` | Copies snapshots and hosts Jupyter |

Data path:

```text
event-4245 PCAP
  -> sender enp7s0
  -> detector-net L2Bridge
  -> receiver enp7s0
  -> wibviz-export
  -> replay-live-http.csv
  -> compressed snapshots
  -> LoomAI Jupyter heatmap
```

## Required files

Sender VM:

```text
/home/ubuntu/icecosmich_run82134054_sub001_evt004245.pcap
```

Receiver VM:

```text
/home/ubuntu/wibviz/wibviz_graph/
/home/ubuntu/wibviz/wibviz_graph/event-4245-channel-map.csv
```

LoomAI:

```text
/home/fabric/work/visualization-wibwiz/graph_notebook/4245.csv
/home/fabric/work/visualization-wibwiz/graph_notebook/live_replay_viewer.ipynb
/home/fabric/work/visualization-wibwiz/graph_notebook/validate_live_replay.ipynb
```

## Start the demonstration

### 1. Start receiver snapshots

Run on `ubuntu@receiver`:

```bash
cd ~/wibviz/wibviz_graph
scripts/live_demo/receiver_snapshot.sh start
```

### 2. Start snapshot synchronization

Run on `fabric@loomai`:

```bash
cd ~/work/visualization-wibwiz/wibviz_graph
scripts/live_demo/loom_sync.sh start
```

The receiver writes a compressed snapshot every three seconds. LoomAI copies
the latest complete snapshot every three seconds using `loomai scp`. Atomic
renames prevent Jupyter from reading a partially transferred gzip file.

### 3. Start the Jupyter viewer

Open `graph_notebook/live_replay_viewer.ipynb` and run its viewer cell. Before
the first packet it reports that it is waiting for live data.

### 4. Start capture

Run on `ubuntu@receiver`:

```bash
cd ~/wibviz/wibviz_graph
scripts/live_demo/receiver_capture.sh
```

The script captures 640 UDP packets destined for port 2049, maps TPC 0 / plane
0, flushes the CSV after each packet, and stops after the event is complete.

### 5. Start replay

Run on `ubuntu@sender`:

```bash
~/wibviz/wibviz_graph/scripts/live_demo/sender_replay.sh
```

The default five-packet-per-second rate makes incremental heatmap updates easy
to observe. Override it with `PPS=10` or another value for performance tests.

## Validate the result

On the receiver:

```bash
cd ~/wibviz/wibviz_graph
scripts/live_demo/validate_output.sh replay-live-http.csv
```

On LoomAI:

```bash
cd ~/work/visualization-wibwiz/wibviz_graph
scripts/live_demo/validate_output.sh /tmp/replay-live-current.csv.gz
```

Both commands must report 409,601 lines and `Validation passed`.

Then run `graph_notebook/validate_live_replay.ipynb`. Its expected final line
is:

```text
PASS: Every detector row matches the reference CSV.
```

The comparison excludes `packet_file` because the reference names a stored
file while live capture identifies `interface:enp7s0`.

## Stop background processes

On `ubuntu@receiver`:

```bash
scripts/live_demo/receiver_snapshot.sh stop
```

On `fabric@loomai`:

```bash
scripts/live_demo/loom_sync.sh stop
```

## Interpreting the partial heatmap

The horizontal axis is detector time tick, not packet arrival time. Each UDP
packet supplies 64 ticks for one hardware slot/stream, and each stream maps to
only part of the detector. The first streams map to high wire IDs, so a slowed
replay initially fills the bottom-left of the heatmap. Later streams fill the
remaining wire bands.

## Known limitations

- The Jupyter view uses polling and typically trails packet arrival by several
  seconds; it is an incremental replay visualization, not zero-latency
  streaming.
- The LoomAI HTTP tunnel returned proxy errors for large responses, so the
  validated path uses compressed atomic snapshots and `loomai scp`.
- The current demonstration processes one event per run.
- Direct detector input, multiple-event rollover, and replay-rate/latency
  characterization remain future work.

