# WibViz

A command-line tool to dissect, analyze, and visualize DUNE WIB (Wireless Interface Board) UDP data streams in real-time. It provides a `curses`-based terminal interface to display channel activities and network statistics. It also provides a library for outputting mappings of ADC channels and their values to their physical wire IDs.

[Demo](https://github.com/dingp/wibwiz/blob/main/wibviz-screen-cap.gif?raw=true)

[Sample heatmap graph of event 4245](https://https://alldaybreakfast.neocities.org/pages/4245.html)

[Sample ridgeline graph of event 4245](https://alldaybreakfast.neocities.org/pages/4245_ridgeline.html)

## Requirements

- Python 3.8+
- `scapy`
- `ifconfig` (for live network statistics on Linux)
- For using the graph_data_extractor, a DUNE DAQ venv and `detchannelmaps` to be available on the venv's PATH. Refer to [detchannelmaps_installation.md](docs/detchannelmaps_installation.md) for further information.

## Installation

1.  Clone the repository or download the source code.
2.  Install the package using pip. From the root directory of the project, run:

    ```bash
    pip install .
    ```
    This will install the `wibviz` command and its dependencies. For development, you can install it in editable mode with `pip install -e .`.

## Usage

The tool can be run in two modes: live sniffing on a network interface or reading from a pre-recorded `.pcap` file.

The tool also provides a library for extracting mapped data from ICERBERG. You can import this library by installing wibviz wherever you want to import the code. Documentation for using the library's methods can be found in [wibviz_GDE_usage.md](docs/wibviz_GDE_usage.md).

### Live Sniffing

To run in live mode, you must specify a network interface. This typically requires root privileges.

```bash
sudo wibviz -i <interface_name>
```
Example:
```bash
sudo wibviz -i bond0.205
```

### Reading from PCAP File

To analyze a `.pcap` file:

```bash
wibviz -f /path/to/your/file.pcap
```

### Exporting graph-ready CSV from a PCAP

The headless exporter parses every tick in each packet, applies the configured
ICEBERG detector channel mapping, and writes graph-ready rows without starting
the curses display:

```bash
wibviz-export \
  --pcap /path/to/event.pcap \
  --output event.csv \
  --tpc 0 \
  --plane 0
```

Use `--limit 1` for an initial environment check. Use `--tpc all` or
`--plane all` to disable either filter. The exporter requires
`run_channel_map_api` and `channel-to-wire` on `PATH`; activate the configured
DUNE DAQ/detchannelmaps environment before running it.

### Live-refreshing heatmap dashboard

Install the optional dashboard dependencies:

```bash
pip install -e '.[dashboard]'
```

Monitor an exporter CSV and refresh the Plotly heatmap whenever the file grows:

```bash
wibviz-dashboard \
  --csv replay-live-4245.csv \
  --host 0.0.0.0 \
  --port 8050 \
  --tpc 0 \
  --plane 0 \
  --refresh-seconds 2
```

The exporter and dashboard are separate processes. Start the dashboard before
or during capture; it waits for complete CSV rows and preserves the user's zoom
between refreshes. To create a portable interactive snapshot without starting
a web server, use:

```bash
wibviz-dashboard \
  --csv replay-live-4245.csv \
  --tpc 0 \
  --plane 0 \
  --snapshot-html replay-live-4245-interactive.html \
  --html-only
```

### Reproducing the FABRIC event-4245 demonstration

Reusable sender, receiver, snapshot, synchronization, and validation commands
are in `scripts/live_demo/`. Follow
[`docs/fabric_live_demo.md`](docs/fabric_live_demo.md) for the required files,
terminal roles, startup order, expected counts, and value-by-value validation.

### Command-Line Options

| Option | Short | Description | Default |
|---|---|---|---|
| `--interface` | `-i` | Network interface to sniff on (live mode). | `bond0.205` |
| `--pcap-file` | `-f` | PCAP file to read from (offline mode). | None |
| `--filter` | | BPF filter for sniffing packets. | `udp and src port 0 and dst port 17476` |
| `--refresh-rate`| | Refresh rate for the display in seconds (live mode). | `1.0` |
| `--prefix` | | Number of packets to process (-1 for infinite). | `-1` |
| `--print-frame-no`| | Print frame numbers to standard output. | Off |
| `--show-dissection`| | Show full Scapy dissection for each packet. | Off |
| `--check` | | Enable consistency checks on packet headers. | Off |
| `--no-parse` | | Disable parsing of WIB headers and data. | Off |
| `--hexdump-frames`| | A space-separated list of frame numbers to hexdump. | None |

## Runtime Control

While the application is running, you can use the following keys:

-   `q`: Quit the application.
-   `+` / `Up Arrow`: Decrease the refresh interval (faster updates).
-   `-` / `Down Arrow`: Increase the refresh interval (slower updates).
