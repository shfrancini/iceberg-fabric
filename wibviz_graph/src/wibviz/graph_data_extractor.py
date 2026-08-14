#!/usr/bin/env python3
import csv
from datetime import datetime
import os
import re
import subprocess
from functools import lru_cache
from pathlib import Path
from typing import Any, Union
from wibviz.tick_offset import TickOffsetHelper
from .schema import GraphStruct

class GraphDataExtractor:
    track_offsets: bool
    tick_offset_helper: TickOffsetHelper

    def __init__(self, track_offsets, verbose=False, mapping_path=None):
        self.track_offsets = track_offsets
        self.verbose = verbose
        self.tick_offset_helper = TickOffsetHelper(on=track_offsets)
        self.channel_map = {}
        self.wire_map = {}

        if mapping_path is None:
            mapping_path = os.environ.get(
                "WIBVIZ_CHANNEL_MAP",
                Path(__file__).resolve().parents[2]
                / "event-4245-channel-map.csv",
            )

        self.mapping_path = Path(mapping_path).expanduser()
        if self.mapping_path.is_file():
            self._load_mapping_file(self.mapping_path)

    def _load_mapping_file(self, mapping_path):
        """Load hardware, offline-channel, and wire mappings once."""
        required = {
            "crate",
            "slot",
            "stream",
            "channel",
            "offline_channel",
            "plane",
            "wire_id",
            "tpc",
        }

        with mapping_path.open(newline="", encoding="utf-8") as mapping_file:
            reader = csv.DictReader(mapping_file)
            missing = required.difference(reader.fieldnames or ())
            if missing:
                columns = ", ".join(sorted(missing))
                raise ValueError(
                    f"mapping file {mapping_path} is missing column(s): {columns}"
                )

            for row in reader:
                hardware = (
                    int(row["crate"]),
                    int(row["slot"]),
                    int(row["stream"]),
                    int(row["channel"]),
                )
                offline_channel = int(row["offline_channel"])
                plane = int(row["plane"])
                wire_id = int(row["wire_id"])
                tpc = int(row["tpc"])

                self.channel_map[hardware] = (offline_channel, plane)
                self.wire_map[offline_channel] = (wire_id, tpc)
    
    def _ensure_scapy_packet(self, packet: Union[bytes, bytearray, Any]) -> Any:
        if isinstance(packet, (bytes, bytearray)):
            from scapy.all import Ether

            return Ether(bytes(packet))
        return packet

    @lru_cache(maxsize=None)
    def _get_offline_channel_and_plane(self, crate, slot, stream, chan):
        mapped = self.channel_map.get((crate, slot, stream, chan))
        if mapped is not None:
            return mapped

        channel_map_result = subprocess.run(
            [
                'run_channel_map_api',
                '--plugin', 'ICEBERGChannelMap',
                '--crate', str(crate),
                '--slot', str(slot),
                '--stream', str(stream),
                '--chan', str(chan),
                '--plane',
            ],
            shell=False, capture_output=True, text=True,
        )

        if channel_map_result.returncode != 0:
            print(f"run_channel_map_api: {channel_map_result.stderr}")
            return -1, -1

        output = channel_map_result.stdout.strip()

        try:
            offline_channel, plane = map(int, output.split())
        except ValueError:
            print(f"Unexpected output from run_channel_map_api.py: {output}")
            return -1, -1

        return offline_channel, plane


    @lru_cache(maxsize=None)
    def _get_wire_info(self, offline_channel):
        mapped = self.wire_map.get(offline_channel)
        if mapped is not None:
            return mapped

        channel_to_wire_result = subprocess.run(
            ['channel-to-wire', str(offline_channel)],
            shell=False, capture_output=True, text=True,
        )

        if channel_to_wire_result.returncode != 0:
            print(f"channel-to-wire: {channel_to_wire_result.stderr}")
            return None

        output = channel_to_wire_result.stdout.strip()
        match = re.search(r'tpc=(\d+).*?wire:\s*(\d+)', output)
        if not match:
            print(f"Unexpected output from channel-to-wire: {output}")
            return None

        tpc = int(match.group(1))
        wire_id = int(match.group(2))
        return wire_id, tpc

    def process_packet_and_return_schema_data(self, data, tpc, plane, pkt):
        packet = self._ensure_scapy_packet(data)
        waveforms = packet['waveforms']
        daqH = packet['daqH']
        if self.verbose:
            print("PACKET: " + str(daqH.timestamp) + ", stream: " + str(daqH.stream_id) + ", slot: " + str(daqH.slot_id))
        schemas = []
        csv_rows = []
        
        for tick_idx, waveform_data in enumerate(waveforms):
            tick_rows, tick_schemas = self._process_tick(
                    tick_idx,
                    waveform_data,
                    tpc,
                    plane,
                    pkt,
                    daqH
                )
            csv_rows.extend(tick_rows)
            schemas.extend(tick_schemas)
            
        if self.track_offsets:
            self.tick_offset_helper.increment_tick_offset_64_for_slot_stream(daqH.slot_id, daqH.stream_id)
            
        return csv_rows, schemas

    def write_schemas_to_csv(self, data, tpc, plane, pkt, with_datestamp=False):
        rows, schemas = self.process_packet_and_return_schema_data(data, tpc, plane, pkt)

        if rows:
            if with_datestamp:
                datestamp = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
                base, ext = os.path.splitext("mapping.csv")
                csv_path = f"{base}_{datestamp}{ext}"
            else:
                csv_path = "mapping.csv"
            needs_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0

            try:
                with open(csv_path, "a") as csv:
                    if needs_header:
                        csv.write("seq_id,timestamp,tick,adc_value,wire_id,tpc,slot,plane,packet_file\n")
                    csv.writelines(rows)
            except Exception as e:
                print("Error writing to CSV: " + str(e))
                
        return None

    def _calculate_wibframechan(self, channel_name):
        match = re.match(r'adc(\d+)ch(\d+)', channel_name)
        
        if not match:
            return None
        
        adc_index = int(match.group(1))
        chan_index = int(match.group(2))
        
        wibframechan = (adc_index * 16) + chan_index
        
        return wibframechan


    def _process_tick(self, tick_idx, waveform_data, tpc, plane, pkt, daqH):
        rows = []
        schemas = []
        
        tick_for_schema = tick_idx
        
        if self.track_offsets:
            tick_offset = self.tick_offset_helper.get_tick_offset_for_slot_stream(daqH.slot_id, daqH.stream_id)
            tick_for_schema = tick_idx + tick_offset.offset
        
        for channel, value in waveform_data.items():
            channel_in_wib = self._calculate_wibframechan(channel)
            if channel_in_wib is None:
                continue
            
            schema = self.build_schema(
                seq_id=daqH.seq_id,
                timestamp=daqH.timestamp,
                tick=tick_for_schema,
                adc_value=value,
                tpc_target=tpc,
                slot=daqH.slot_id,
                plane_target=plane,
                crate=daqH.crate_id,
                stream=daqH.stream_id,
                chan=channel_in_wib,
                packet_file=pkt
            )

            if schema:
                rows.append(
                    f"{schema.seq_id}, {schema.timestamp}, {schema.tick}, {schema.adc_value}, {schema.wire_id}, {schema.tpc}, {schema.slot}, {schema.plane}, {schema.packet_file}\n"
                )
                schemas.append(schema)

        return rows, schemas

    def build_schema(self, seq_id, timestamp, tick, adc_value, tpc_target, plane_target, crate, slot, stream, chan, packet_file):
        offline_channel, plane_on = self._get_offline_channel_and_plane(crate, slot, stream, chan)
        
        if offline_channel < 0 or (plane_target is not None and plane_on != plane_target):
            return None

        wire_info = self._get_wire_info(offline_channel)
        
        if wire_info is None:
            return None

        wire_id, tpc_on = wire_info
        
        if tpc_target is not None and tpc_on != tpc_target:
            return None
        
        final_tpc = tpc_on if tpc_target is None else tpc_target
        final_plane = plane_on if plane_target is None else plane_target
        
        return GraphStruct(
            seq_id=seq_id,
            timestamp=timestamp,
            tick=tick,
            adc_value=adc_value,
            wire_id=wire_id,
            tpc=final_tpc,
            slot=slot,
            plane=final_plane,
            packet_file=packet_file
        )
