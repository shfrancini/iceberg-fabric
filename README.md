# ICEBERG FABRIC Public Baseline

This repository packages a portable baseline for replaying ICEBERG/WIB packet
data across a FABRIC slice, decoding it with WibViz, validating the decoded CSV,
and producing post-run visualizations.

The public baseline is intentionally conservative. It uses one receiver-side
decoder process, fixed 64-packet chunks, bounded queues, ordered output, and
exact CSV validation. It does not create or modify FABRIC slices and it does not
include large PCAP data.

## Baseline Architecture

```text
sender VM pcaps + tcpreplay
        |
        v
receiver VM tcpdump
        |
        v
64-packet raw chunks -> bounded raw queue, capacity 4
        |
        v
one decoder controller -> one persistent receiver-side decoder
        |
        v
ordered CSV append -> exact validation -> heatmap and waveform images
```

Default runtime contract:

- `source_mode: pcap_replay`
- `decode.chunking.chunk_packets: 64`
- `decode.queue.capacity_chunks: 4`
- `decode.queue.decode_workers: 1`
- `decode.queue.decode_worker_mode: persistent_receiver_process`
- `decode.queue.persistent_batch_chunks: 1`
- `output.write_strategy: ordered_append`
- `validation.enabled: true`

Multi-chunk persistent batching remains configurable through
`decode.queue.persistent_batch_chunks`, but values greater than `1` are
experimental. Do not add a second decoder for the public baseline.

## Prerequisites

Local machine or notebook:

- Python 3.10 or newer
- Git
- `ssh` and `scp` for `transport.type: ssh`, or the `loomai` CLI for
  `transport.type: loom`
- Enough disk space for the clone, local reports, copied decoded chunks, and
  generated visualizations

Sender VM for replay:

- `python3`
- `tcpreplay`
- readable user-provided PCAP
- correct data-plane interface name

Receiver VM:

- `python3`
- `tcpdump`
- non-interactive capture permission, usually passwordless `sudo tcpdump`
- WibViz source path configured in YAML
- readable mapping CSV and, when validation is enabled, reference CSV
- writable output directory

## Installation

From a clean clone, install the package and all optional test, dashboard, and
notebook dependencies with one command:

```bash
python3 -m pip install -e '.[dev,dashboard,notebook]'
```

For a smaller runtime-only install, use `python3 -m pip install -e .`. The
dashboard and notebook extras are only needed when using Plotly/Dash dashboards
or Jupyter notebooks. See [docs/installation.md](docs/installation.md).

## Local Demonstration

The local demo uses synthetic data only. It does not read a PCAP, contact
FABRIC, or require credentials.

```bash
iceberg-fabric demo --output /tmp/iceberg_fabric_readme_demo
```

Expected outputs include:

- `/tmp/iceberg_fabric_readme_demo/synthetic_wib_rows.csv`
- `/tmp/iceberg_fabric_readme_demo/synthetic_heatmap.png`
- `/tmp/iceberg_fabric_readme_demo/synthetic_waveforms.png`
- `/tmp/iceberg_fabric_readme_demo/summary.json`

## Configuration

Start from the public template and change only YAML values:

```bash
cp configs/single_decoder_baseline_template.yaml configs/my_baseline.yaml
```

Set the transport, slice and node names, SSH host details if needed, data-plane
interfaces, capture filter, PCAP path, mapping path, validation reference path,
output directory, input rate, and packet limit. The template uses finite,
low-rate example values; it is not a 10,000 pps stress configuration.

Two event examples are included:

- `configs/event_6860_finite_benchmark_example.yaml` is a finite benchmark
  example that requires user-provided PCAP, mapping, and reference data.
- `configs/event_4245_legacy_example.yaml` preserves the historical event 4245
  shape for comparison with older public artifacts.

See [docs/configuration_reference.md](docs/configuration_reference.md).

## Preflight

Run preflight before any replay:

```bash
iceberg-fabric preflight --config configs/my_baseline.yaml
```

For local validation of the YAML and Python environment without contacting
sender or receiver hosts:

```bash
iceberg-fabric preflight --config configs/single_decoder_baseline_template.yaml --local-only
```

Preflight prints `PASS`, `FAIL`, or `WARNING` for schema, dependencies,
transport commands, reachability, remote tools, interfaces, capture permission,
input paths, WibViz paths, output permissions, and disk space. Any required
failure exits nonzero and includes a repair instruction.

## Pipeline Dry-Run

Dry-run plans the pipeline, writes manifest/report placeholders, and does not
start capture, replay, or decode:

```bash
iceberg-fabric run --config configs/single_decoder_baseline_template.yaml --dry-run
```

Use this to verify the planned run ID, transport, remote paths, chunking,
validation reference, and output locations before contacting a slice.

## Bounded FABRIC Smoke Test

After preflight passes against your real slice and user-provided data, run a
small bounded replay:

```bash
iceberg-fabric run --config configs/my_baseline.yaml --max-live-chunks 2
```

For replay mode this caps the sender to at most two 64-packet chunks, subject to
any lower `pcap_replay.packet_limit` already set in the config.

## Full User-Provided Replay

Run the full configured replay only after the dry-run and bounded smoke test
match your expected paths and interfaces:

```bash
iceberg-fabric run --config configs/my_baseline.yaml
```

For repeated finite runs, use:

```bash
iceberg-fabric run --config configs/my_baseline.yaml --repeat 3
```

The repository does not ship large or legally uncertain PCAPs. Users must
provide their own input PCAP, channel mapping CSV, and validation reference CSV.

## Outputs

Each run writes a timestamped directory under `paths.output_root` unless
`--output-root` is supplied:

```text
runs/<run_id>/
  artifacts/combined.csv
  artifacts/decoded_chunks/
  reports/summary.json
  reports/manifest.json
  reports/measurement_summary.csv
  reports/queue_metrics_history.csv
  reports/validation_result.json
  visualizations/heatmap.png
  visualizations/stacked_waveform.png
```

Metrics include throughput, capture-to-output latency, queue depth,
backpressure, decode timing, batch accounting, output ordering, validation
status, and pass/fail criteria. See [docs/output_schema.md](docs/output_schema.md).

## Known Limitations

- The runner assumes an existing FABRIC slice. It does not provision, renew, or
  modify slices.
- Preflight can check remote readiness, but it does not install VM packages or
  copy PCAP/reference data.
- Exact validation requires a reference CSV matching the selected PCAP and
  decode filters.
- `live_nic` is available for bounded captures but remains less standardized
  than finite PCAP replay.
- Multi-chunk persistent batching is experimental and must stay single-decoder.
- Historical evidence files describe recorded runs; they are not general
  throughput guarantees.

## Additional Documentation

- [docs/installation.md](docs/installation.md)
- [docs/configuration_reference.md](docs/configuration_reference.md)
- [docs/output_schema.md](docs/output_schema.md)
- [docs/troubleshooting.md](docs/troubleshooting.md)
- [docs/architecture.md](docs/architecture.md)

## Acknowledgment

This work began with repository and project context shared by Kels Cavin. The
packet visualization path is based on WibViz and adapted here for a portable
FABRIC public baseline.
