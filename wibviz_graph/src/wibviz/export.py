import argparse
import csv
import os
import shutil
import struct
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any


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


def _daq_header_value(daq_header: Any, field: str) -> int:
    if isinstance(daq_header, dict):
        return int(daq_header[field])
    return int(getattr(daq_header, field))


def decoded_packet_sort_key(parsed_packet: dict, original_index: int = 0) -> tuple[int, int, int, int, int]:
    """Return detector-order key for a decoded packet.

    High-rate capture may reorder packets inside chunk PCAPs; tick
    reconstruction must not depend on capture order. Sort by stable DAQ
    metadata before TickOffsetHelper assigns per-slot/stream tick ranges.
    """
    daq_header = parsed_packet["daqH"]
    return (
        _daq_header_value(daq_header, "slot_id"),
        _daq_header_value(daq_header, "stream_id"),
        _daq_header_value(daq_header, "timestamp"),
        _daq_header_value(daq_header, "seq_id"),
        int(original_index),
    )


def sort_decoded_packet_records(records: list[tuple[int, dict]]) -> list[tuple[int, dict]]:
    """Sort decoded packet records without dropping or deduplicating packets."""
    return sorted(
        records,
        key=lambda item: decoded_packet_sort_key(item[1], item[0]),
    )


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
    rows = [
        [
            getattr(schema, field)
            for field in CSV_FIELDS
        ]
        for schema in schemas
    ]

    if rows:
        writer.writerows(rows)

    return len(rows)


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


def export_summary_line(stats, output_path):
    """Return the historical CLI export summary line for parsed stats."""
    return (
        "Export complete: "
        f"{stats.packets_read} packet(s) read, "
        f"{stats.packets_parsed} parsed, "
        f"{stats.packets_skipped} skipped, "
        f"{stats.rows_written} CSV row(s) written "
        f"to {output_path}"
    )


@dataclass
class ExportContext:
    """Reusable decode/export context for persistent worker processes.

    The context keeps imports and channel-map construction alive across chunks.
    Chunk-local state is reset before each PCAP export to preserve the old
    per-process `export_pcap()` semantics.
    """

    tpc: int | None = 0
    plane: int | None = 0
    track_offsets: bool = True
    verbose: bool = False
    mapping_path: str | os.PathLike | None = None
    skip_mapping_check: bool = False
    sort_packets: bool = True

    def __post_init__(self):
        check_common_export_inputs(
            mapping_path=self.mapping_path,
            skip_mapping_check=self.skip_mapping_check,
        )

        from scapy.all import PcapReader
        from .graph_data_extractor import GraphDataExtractor
        from .processing import Processor

        self.pcap_reader_cls = PcapReader
        self.processor_cls = Processor
        self.extractor = GraphDataExtractor(
            track_offsets=self.track_offsets,
            verbose=self.verbose,
            mapping_path=self.mapping_path,
        )

    def new_processor(self):
        return self.processor_cls(
            build_processor_config(
                print_frame_no=self.verbose,
            )
        )

    def reset_for_chunk(self):
        reset = getattr(self.extractor, "reset_for_chunk", None)
        if reset is not None:
            reset()

    def export_pcap(
        self,
        pcap_path,
        output_path,
        *,
        limit=None,
        append=False,
        sort_packets=None,
        packet_label=None,
    ):
        if limit is not None and limit < 1:
            raise ValueError("limit must be at least 1")

        pcap_path = Path(pcap_path)
        output_path = Path(output_path)

        if not pcap_path.is_file():
            raise FileNotFoundError(
                f"PCAP file not found: {pcap_path}"
            )

        output_path, mode, needs_header = prepare_export(
            output_path,
            append=append,
        )

        sort_enabled = self.sort_packets if sort_packets is None else sort_packets
        packet_label = str(pcap_path) if packet_label is None else packet_label
        processor = self.new_processor()
        self.reset_for_chunk()
        stats = ExportStats()

        with self.pcap_reader_cls(str(pcap_path)) as packets, output_path.open(
            mode,
            newline="",
            encoding="utf-8",
        ) as output_file:
            writer = csv.writer(output_file)

            if needs_header:
                writer.writerow(CSV_FIELDS)

            decoded_packets = []

            for packet in packets:
                if limit is not None and stats.packets_read >= limit:
                    break

                original_index = stats.packets_read
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

                    if self.verbose:
                        print(
                            f"Skipping packet {stats.packets_read}: "
                            f"{type(error).__name__}: {error}"
                        )

                    continue

                if parsed is None:
                    stats.packets_skipped += 1
                    continue

                stats.packets_parsed += 1
                decoded_packets.append((original_index, parsed))

            if sort_enabled:
                decoded_packets = sort_decoded_packet_records(decoded_packets)

            for _original_index, parsed in decoded_packets:
                _, schemas = (
                    self.extractor.process_packet_and_return_schema_data(
                        parsed,
                        tpc=self.tpc,
                        plane=self.plane,
                        pkt=packet_label,
                        include_csv_rows=False,
                    )
                )

                stats.rows_written += write_schema_rows(
                    writer,
                    schemas,
                )

        return stats


def export_pcap_with_context(
    context,
    pcap_path,
    output_path,
    *,
    limit=None,
    append=False,
    sort_packets=None,
    packet_label=None,
):
    """Export one PCAP using a reusable `ExportContext`."""
    return context.export_pcap(
        pcap_path,
        output_path,
        limit=limit,
        append=append,
        sort_packets=sort_packets,
        packet_label=packet_label,
    )


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

    try:
        _, schemas = extractor.process_packet_and_return_schema_data(
            parsed,
            tpc=tpc,
            plane=plane,
            pkt=packet_label,
            include_csv_rows=False,
        )
    except TypeError as error:
        if "include_csv_rows" not in str(error):
            raise
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
    sort_packets=True,
):
    """Parse a WIB PCAP and export mapped graph rows to CSV."""
    context = ExportContext(
        tpc=tpc,
        plane=plane,
        track_offsets=track_offsets,
        verbose=verbose,
        mapping_path=mapping_path,
        skip_mapping_check=skip_mapping_check,
        sort_packets=sort_packets,
    )
    return export_pcap_with_context(
        context,
        pcap_path,
        output_path,
        limit=limit,
        append=append,
        sort_packets=sort_packets,
        packet_label=str(Path(pcap_path)),
    )

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
    flush_every_packets=1,
):
    """Sniff live WIB packets from a NIC and export mapped graph rows."""
    if flush_every_packets < 1:
        raise ValueError("flush_every_packets must be at least 1")

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
            if stats.packets_read % flush_every_packets == 0:
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

        output_file.flush()

    return stats

    


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Convert DUNE WIB packets from a PCAP "
            "file into graph-ready CSV rows."
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
        "--flush-every-packets",
        type=int,
        default=1,
        help=(
            "Flush the output CSV every N handled packets in live mode. "
            "The default preserves historical flush-after-every-packet behavior."
        ),
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
        "--preserve-pcap-order",
        action="store_true",
        help=(
            "Decode PCAP packets in capture order. By default PCAP packets are "
            "sorted by DAQ slot, stream, timestamp, and seq_id before tick "
            "assignment so high-rate chunk captures are deterministic."
        ),
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print packet progress and skipped-packet errors.",
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
            "Skip checking for DUNE channel-mapping commands when no "
            "mapping CSV is available."
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
                sort_packets=not args.preserve_pcap_order,
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
                flush_every_packets=args.flush_every_packets,
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

    print(export_summary_line(stats, args.output))

    return 0


if __name__ == "__main__":
    sys.exit(main())