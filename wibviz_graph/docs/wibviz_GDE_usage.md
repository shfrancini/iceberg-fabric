## Graph Data Extractor for Wibviz
In this forked version of Wibviz (developed in Spring 2026, updated to independent version 0.2.1), a library to extract mapped ADC values was added. The driving code for this library is the `graph_data_extractor.py`. 

The GDE can extract data to a set of `GraphStruct` objects from `schema.py` or dump data to a CSV from a DUNE packet (or both!)

The GDE *requires* access to a modified version of the DUNE DAQ library. Refer to [detchannelmaps_installation.md](detchannelmaps_installation.md) for more information.

---

### Using the library
1. Follow the instructions in the README.md to install wibviz with pip.
2. Add `from graph_data_extractor import GraphDataExtractor` to your project's import statements.
3. Create a graph_data_extractor object: `gde = GraphDataExtractor(track_offsets=?)`
    - `track_offsets` can be set to true or false. Offset tracking is linear and only works if the extractor is running on **one node and one thread.** If you are distributing this work across multiple machines, offset tracking will be wrong. As of this version of wibviz, you must sanitize/order your data after the fact.
4. Review the API reference below and utilize the library as needed.

---

### Potential Improvements
This library was developed by an undergraduate student in one semester, and as such could be improved in several places.
- Better code quality and separation of concerns, refactoring methods to not return tuples
- Although running subprocesses for the channel mapping calls is somewhat optimized with lru_cache, it could be improved, or made compatible with threading.
- Compatability with other DUNE mapping plugins and schemas, since right now, just ICEBERG is supported.
- An option to write a packet's CSV output from the CLI.
---

## API Reference

#### Constructor: `GraphDataExtractor(track_offsets)`
Initialize a new GraphDataExtractor instance.

**Parameters:**
- `track_offsets` (bool): Whether to track tick offsets across packets. Set to `True` only when processing packets sequentially on a single thread/node. Set to `False` for distributed processing.

**Returns:** GraphDataExtractor instance

**Example:**
```python
gde = GraphDataExtractor(track_offsets=True)
```

---

#### Method: `process_packet_and_return_schema_data(data, tpc, plane, pkt)`
Process a single packet and extract ADC waveform data, returning both schema objects and CSV-formatted rows.

**Parameters:**
- `data` (bytes, bytearray, or Scapy packet): The raw packet data or a Scapy Ether packet object. If raw bytes are provided, they will be converted to a Scapy packet automatically.
- `tpc` (int): The target TPC to filter for. Only waveforms belonging to this TPC will be included. `None` will fetch all TPCs.
- `plane` (int): The target plane ID to filter for (e.g., induction, collection plane). Passed as an integer, 0, 1, 2. `None` will fetch all planes.
- `pkt` (str): A packet identifier string (e.g., filename) to include in the output for tracking purposes.

**Returns:** Tuple of `(csv_rows, schemas)`
- `csv_rows` (list of str): List of comma-separated formatted strings, one per ADC value, ready to write to CSV. Each row contains: `seq_id, timestamp, tick, adc_value, wire_id, tpc, slot, plane, packet_file`
- `schemas` (list of GraphStruct): List of schema objects containing the structured data. Each schema has fields: `seq_id`, `timestamp`, `tick`, `adc_value`, `wire_id`, `tpc`, `slot`, `plane`, `packet_file`

**Notes:**
- If `track_offsets=True` was set during initialization, tick offsets will be automatically incremented after processing.
- Waveforms that don't match the specified `tpc` and `plane` are filtered out (returns `None`).

**Example:**
```python
csv_rows, schema_objects = gde.process_packet_and_return_schema_data(
    data=packet_bytes,
    tpc=0,
    plane=1,
    pkt="event_001.pcap"
)
```

---

#### Method: `write_schemas_to_csv(data, tpc, plane, pkt, with_datestamp=False)`
Process a packet and write extracted data directly to a CSV file.

**Parameters:**
- `data` (bytes, bytearray, or Scapy packet): The raw packet data or a Scapy Ether packet object.
- `tpc` (int): The target TPC ID to filter for. `None` will fetch all TPCs.
- `plane` (int): The target plane ID to filter for. `None` will fetch all planes.
- `pkt` (str): A packet identifier string to include in output.
- `with_datestamp` (bool, optional): If `True`, appends a timestamp to the CSV filename in format `mapping_YYYY-MM-DD-HH-MM-SS.csv`. Default is `False`, which uses `mapping.csv`.

**Returns:** `None`

**Side Effects:**
- Creates or appends to `mapping.csv` (or `mapping_YYYY-MM-DD-HH-MM-SS.csv` if `with_datestamp=True`).
- Automatically writes header row on first write or if file is empty.
- Prints error messages to stdout if CSV writing fails.

**Notes:**
- This method wraps `process_packet_and_return_schema_data()` and then writes results to disk.
- If no rows are produced (all waveforms filtered), the file is not modified.

**Example:**
```python
gde.write_schemas_to_csv(
    data=packet_bytes,
    tpc=0,
    plane=1,
    pkt="event_001.pcap",
    with_datestamp=True
)
```

---

#### Method: `build_schema(seq_id, timestamp, tick, adc_value, tpc_target, plane_target, crate, slot, stream, chan, packet_file)`
Build a single GraphStruct schema object from raw ADC data. This method performs channel mapping and validation.

**Parameters:**
- `seq_id` (int): Sequence ID from the DAQ header.
- `timestamp` (int): Timestamp value from the DAQ header.
- `tick` (int): Tick value (with offset applied if tracking is enabled).
- `adc_value` (int): Raw ADC value.
- `tpc_target` (int or None): Expected TPC ID to filter for. `None` will fetch data from all TPCs.
- `plane_target` (int or None): Expected plane ID to filter for. `None` will fetch data from all planes.
- `crate` (int): Crate ID from the DAQ header.
- `slot` (int): Slot ID from the DAQ header.
- `stream` (int): Stream ID from the DAQ header.
- `chan` (int): Channel index in WIB frame (0-127).
- `packet_file` (str): Packet identifier for tracking.

**Returns:** GraphStruct object or `None`
- On success: Returns a `GraphStruct` object with all fields populated including the mapped wire ID. When `tpc_target` or `plane_target` are `None`, the returned schema uses the actual values from the wire mapping.
- On failure: Returns `None` if channel mapping fails or if the TPC/plane doesn't match the target filter (when specified).

**Notes:**
- This method is called internally by `process_packet_and_return_schema_data()` for each ADC value.
- Channel mapping requires external tools (`run_channel_map_api` and `channel-to-wire`) to be available in the environment.
- Results are cached (LRU cache) to avoid redundant channel mapping lookups.
- When `tpc_target=None`, all TPCs are accepted and the actual TPC from the wire mapping is used in the returned schema.
- When `plane_target=None`, all planes are accepted and the actual plane from the wire mapping is used in the returned schema.

**Example:**
```python
# Get data only from TPC 0, plane 1
schema = gde.build_schema(
    seq_id=12345,
    timestamp=1000000,
    tick=42,
    adc_value=1234,
    tpc_target=0,
    plane_target=1,
    crate=0,
    slot=2,
    stream=4,
    chan=32,
    packet_file="event_001.pcap"
)

# Get data from all TPCs and plane 1
schema = gde.build_schema(
    seq_id=12345,
    timestamp=1000000,
    tick=42,
    adc_value=1234,
    tpc_target=None,  # Accept all TPCs
    plane_target=1,
    crate=0,
    slot=2,
    stream=4,
    chan=32,
    packet_file="event_001.pcap"
)

if schema:
    print(f"Wire ID: {schema.wire_id}, TPC: {schema.tpc}, Plane: {schema.plane}")
```