import argparse
import csv
import os
import shutil
import struct
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace


CSV_FIELDS = (
    "seq_id",
    "timestamp",
    "tick",
    "adc_value",
    "wire_id",
    "tpc",
    "slot",
    "plane",
    "packet_file",
)

REQUIRED_MAPPING_COMMANDS = (
    "run_channel_map_api",
    "channel-to-wire",
)


@dataclass
class ExportStats:
    packets_read: int = 0
    packets_parsed: int = 0
    rows_written: int = 0
    packets_skipped: int = 0


def parse_filter(value):
    """Convert a TPC/plane argument to an integer or None for 'all'."""
    if value.lower() == "all":
        return None

    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "expected a non-negative integer or 'all'"
        ) from error

    if parsed < 0:
        raise argparse.ArgumentTypeError(
            "expected a non-negative integer or 'all'"
        )

    return parsed


def missing_mapping_commands():
    """Return any required channel-mapping commands missing from PATH."""
    return [
        command
        for command in REQUIRED_MAPPING_COMMANDS
        if shutil.which(command) is None
    ]


def write_schema_rows(writer, schemas):
    """Write GraphStruct objects to CSV and return the number written."""
    count = 0

    for schema in schemas:
        writer.writerow([
            getattr(schema, field)
            for field in CSV_FIELDS
        ])
        count += 1

    return count


def build_processor_config(print_frame_no=False):
    """Build the configuration object expected by Processor."""
    return SimpleNamespace(
        prefix=-1,
        print_frame_no=print_frame_no,
        hexdump_frames=None,
        parse=True,
        check=False,
        show_dissection=False,
    )


def check_common_export_inputs(
    limit=None,
    mapping_path=None,
    skip_mapping_check=False,
):
    """Validate arguments/environment shared by PCAP and live modes."""
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")

    if mapping_path is not None and Path(mapping_path).expanduser().is_file():
        return

    if skip_mapping_check:
        return

    missing = missing_mapping_commands()

    if missing:
        commands = ", ".join(missing)
        raise RuntimeError(
            f"missing DUNE channel-mapping command(s): {commands}. "
            "Activate the DUNE DAQ/detchannelmaps environment first."
        )


def prepare_export(output_path, append=False):
    """Create the output directory and return CSV open mode/header flag."""
    output_path = Path(output_path)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    needs_header = (
        not append
        or not output_path.exists()
        or output_path.stat().st_size == 0
    )

    mode = "a" if append else "w"

    return output_path, mode, needs_header


def process_one_packet(
    packet,
    processor,
    extractor,
    writer,
    packet_label,
    stats,
    tpc=0,
    plane=0,
    verbose=False,
):
    """Decode one packet, write any mapped rows, and update stats."""
    stats.packets_read += 1

    try:
        parsed = processor.process_packet_all_ticks(packet)
    except (
        IndexError,
        KeyError,
        TypeError,
        ValueError,
        struct.error,
    ) as error:
        stats.packets_skipped += 1

        if verbose:
            print(
                f"Skipping packet {stats.packets_read}: "
                f"{type(error).__name__}: {error}"
            )

        return

    if parsed is None:
        stats.packets_skipped += 1
        return

    stats.packets_parsed += 1

    _, schemas = extractor.process_packet_and_return_schema_data(
        parsed,
        tpc=tpc,
        plane=plane,
        pkt=packet_label,
    )

    stats.rows_written += write_schema_rows(
        writer,
        schemas,
    )


def export_packets(
    packets,
    processor,
    extractor,
    writer,
    packet_label,
    tpc=0,
    plane=0,
    limit=None,
    verbose=False,
):
    """Export packets from any iterable source: PCAP reader, list, etc."""
    stats = ExportStats()

    for packet in packets:
        if limit is not None and stats.packets_read >= limit:
            break

        process_one_packet(
            packet,
            processor,
            extractor,
            writer,
            packet_label,
            stats,
            tpc=tpc,
            plane=plane,
            verbose=verbose,
        )

    return stats


def export_pcap(
    pcap_path,
    output_path,
    tpc=0,
    plane=0,
    track_offsets=True,
    limit=None,
    append=False,
    verbose=False,
    mapping_path=None,
    skip_mapping_check=False,
):
    """Parse a WIB PCAP and export mapped graph rows to CSV."""
    pcap_path = Path(pcap_path)

    if not pcap_path.is_file():
        raise FileNotFoundError(
            f"PCAP file not found: {pcap_path}"
        )

    check_common_export_inputs(
        limit=limit,
        mapping_path=mapping_path,
        skip_mapping_check=skip_mapping_check,
    )

    from scapy.all import PcapReader
    from .graph_data_extractor import GraphDataExtractor
    from .processing import Processor

    output_path, mode, needs_header = prepare_export(
        output_path,
        append=append,
    )

    processor = Processor(
        build_processor_config(
            print_frame_no=verbose
        )
    )

    extractor = GraphDataExtractor(
        track_offsets=track_offsets,
        verbose=verbose,
        mapping_path=mapping_path,
    )

    with PcapReader(str(pcap_path)) as packets, output_path.open(
        mode,
        newline="",
        encoding="utf-8",
    ) as output_file:
        writer = csv.writer(output_file)

        if needs_header:
            writer.writerow(CSV_FIELDS)

        stats = export_packets(
            packets,
            processor,
            extractor,
            writer,
            packet_label=str(pcap_path),
            tpc=tpc,
            plane=plane,
            limit=limit,
            verbose=verbose,
        )

    return stats


def export_interface(
    interface,
    output_path,
    tpc=0,
    plane=0,
    track_offsets=True,
    limit=None,
    append=False,
    verbose=False,
    bpf_filter="udp and src port 0 and dst port 17476",
    timeout=None,
    mapping_path=None,
    skip_mapping_check=False,
):
    """Sniff live WIB packets from a NIC and export mapped graph rows."""
    check_common_export_inputs(
        limit=limit,
        mapping_path=mapping_path,
        skip_mapping_check=skip_mapping_check,
    )

    from scapy.all import sniff
    from .graph_data_extractor import GraphDataExtractor
    from .processing import Processor

    output_path, mode, needs_header = prepare_export(
        output_path,
        append=append,
    )

    processor = Processor(
        build_processor_config(
            print_frame_no=verbose
        )
    )

    extractor = GraphDataExtractor(
        track_offsets=track_offsets,
        verbose=verbose,
        mapping_path=mapping_path,
    )

    stats = ExportStats()

    with output_path.open(
        mode,
        newline="",
        encoding="utf-8",
    ) as output_file:
        writer = csv.writer(output_file)

        if needs_header:
            writer.writerow(CSV_FIELDS)

        def handle_packet(packet):
            process_one_packet(
                packet,
                processor,
                extractor,
                writer,
                packet_label=f"interface:{interface}",
                stats=stats,
                tpc=tpc,
                plane=plane,
                verbose=verbose,
            )
            output_file.flush()

        try:
            sniff(
                iface=interface,
                filter=bpf_filter,
                prn=handle_packet,
                count=limit or 0,
                timeout=timeout,
                store=False,
            )
        except KeyboardInterrupt:
            if verbose:
                print("Stopping live export after keyboard interrupt.")

    return stats


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Convert DUNE WIB packets from a PCAP file or live "
            "network interface into graph-ready CSV rows."
        )
    )

    source = parser.add_mutually_exclusive_group(required=True)

    source.add_argument(
        "--pcap",
        help="Input PCAP file.",
    )

    source.add_argument(
        "--interface",
        help="Network interface to sniff from.",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Destination CSV file.",
    )

    parser.add_argument(
        "--tpc",
        type=parse_filter,
        default=0,
        help="TPC to include, or 'all' (default: 0).",
    )

    parser.add_argument(
        "--plane",
        type=parse_filter,
        default=0,
        help="Plane to include, or 'all' (default: 0).",
    )

    parser.add_argument(
        "--no-track-offsets",
        action="store_false",
        dest="track_offsets",
        help="Keep packet ticks at 0-63.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        help="Process at most this many packets.",
    )

    parser.add_argument(
        "--append",
        action="store_true",
        help="Append instead of replacing the CSV.",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print packet progress and skipped-packet errors.",
    )

    parser.add_argument(
        "--bpf-filter",
        default="udp and src port 0 and dst port 17476",
        help="BPF filter for live interface sniffing.",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        help="Stop live sniffing after this many seconds.",
    )

    parser.add_argument(
        "--mapping",
        default=os.environ.get(
            "WIBVIZ_CHANNEL_MAP",
            str(
                Path(__file__).resolve().parents[2]
                / "event-4245-channel-map.csv"
            ),
        ),
        help=(
            "CSV channel map. Defaults to WIBVIZ_CHANNEL_MAP or "
            "event-4245-channel-map.csv in the project root."
        ),
    )

    parser.add_argument(
        "--skip-mapping-check",
        action="store_true",
        help=(
            "Skip the startup check for external mapping commands. "
            "Use only when a valid CSV mapping is configured."
        ),
    )

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.pcap:
            stats = export_pcap(
                pcap_path=args.pcap,
                output_path=args.output,
                tpc=args.tpc,
                plane=args.plane,
                track_offsets=args.track_offsets,
                limit=args.limit,
                append=args.append,
                verbose=args.verbose,
                mapping_path=args.mapping,
                skip_mapping_check=args.skip_mapping_check,
            )
        else:
            stats = export_interface(
                interface=args.interface,
                output_path=args.output,
                tpc=args.tpc,
                plane=args.plane,
                track_offsets=args.track_offsets,
                limit=args.limit,
                append=args.append,
                verbose=args.verbose,
                bpf_filter=args.bpf_filter,
                timeout=args.timeout,
                mapping_path=args.mapping,
                skip_mapping_check=args.skip_mapping_check,
            )
    except (
        FileNotFoundError,
        RuntimeError,
        ValueError,
        OSError,
    ) as error:
        parser.exit(
            1,
            f"wibviz-export: error: {error}\n",
        )

    print(
        "Export complete: "
        f"{stats.packets_read} packet(s) read, "
        f"{stats.packets_parsed} parsed, "
        f"{stats.packets_skipped} skipped, "
        f"{stats.rows_written} CSV row(s) written "
        f"to {args.output}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
