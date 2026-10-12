# Configuration Reference

Start from `configs/single_decoder_baseline_template.yaml` and edit YAML only.
The template values are examples, not active slice details.

## Top-Level Fields

| Field | Required | Meaning |
| --- | --- | --- |
| `schema_version` | yes | Public config schema version. Current value is `1`. |
| `label` | yes | Run label used in run IDs and plot titles. |
| `event_id` | no | Human-readable event identifier for reports. |
| `source_mode` | yes | `pcap_replay` or `live_nic`. |

## `transport`

| Field | Required | Meaning |
| --- | --- | --- |
| `type` | yes | `ssh` or `loom`. |
| `username` | ssh | SSH username for both VMs. |
| `ssh_key` | ssh | Local SSH private key path. |
| `ssh_options` | no | Extra `ssh` and `scp` arguments such as `-F` config or `ProxyJump`. |
| `copy_tool` | ssh | `scp` by default; `rsync` is accepted when installed. |
| `hosts` | ssh | Map logical node names to management hostnames or IP addresses. |

For LoomAI:

```yaml
transport:
  type: loom
slice:
  name: your-fabric-slice
  sender_node: sender
  receiver_node: receiver
```

For SSH:

```yaml
transport:
  type: ssh
  username: ubuntu
  ssh_key: ~/.ssh/fabric_slice_key
  hosts:
    sender: sender.example.fabric.testbed.net
    receiver: receiver.example.fabric.testbed.net
```

## `slice`

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | loom | Existing FABRIC slice name. |
| `sender_node` | replay | Logical sender node name. |
| `receiver_node` | yes | Logical receiver node name. |

The runner does not create, submit, renew, or modify slices.

## `network`

| Field | Required | Meaning |
| --- | --- | --- |
| `sender_interface` | replay | Sender data-plane interface for `tcpreplay`. |
| `receiver_interface` | yes | Receiver data-plane interface for `tcpdump`. |
| `capture_filter` | yes | BPF expression used by receiver capture. |
| `expected_sender_ip` | no | Optional guardrail documented in reports. |
| `expected_receiver_ip` | no | Optional guardrail documented in reports. |

## `paths`

| Field | Required | Location | Meaning |
| --- | --- | --- | --- |
| `output_root` | yes | local | Root for local copied reports and visualizations. |
| `receiver_root` | yes | receiver VM | Base directory for receiver run state. |
| `sender_root` | replay | sender VM | Base directory for sender run state. |
| `wibviz_src` | yes | receiver VM | Directory containing the receiver-importable `wibviz` package. |
| `mapping_csv` | yes | receiver VM | Channel mapping CSV used by WibViz export. |
| `report_mirror_root` | no | local | Optional local mirror for run report bundles. |

## `pcap_replay`

| Field | Required | Meaning |
| --- | --- | --- |
| `sender_pcap` | replay | Absolute sender VM path to the user-provided PCAP. |
| `replay_timing` | replay | `pps` or `original`. |
| `pps` | when timing is pps | Finite input rate for `tcpreplay`. |
| `packet_limit` | no | Maximum packets to send for bounded runs. |
| `tcpreplay_extra_args` | no | Additional explicit arguments for `tcpreplay`. |

The public template uses a finite low-rate example. The 10,000 pps stress run is
not a public default.

## `decode`

| Field | Required | Baseline Value |
| --- | --- | --- |
| `tpc` | yes | User-selected TPC or `all`. |
| `plane` | yes | User-selected plane or `all`. |
| `chunking.mode` | yes | `fixed_packet_count`. |
| `chunking.chunk_packets` | yes | `64`. |
| `queue.pipeline_mode` | yes | `concurrent`. |
| `queue.decode_workers` | yes | `1`. |
| `queue.decode_worker_mode` | yes | `persistent_receiver_process`. |
| `queue.capacity_chunks` | yes | `4`. |
| `queue.persistent_batch_chunks` | yes | `1`. |
| `queue.require_bounded_queues` | yes | `true`. |

`persistent_batch_chunks` may be raised for experimental multi-chunk batching,
but it must not exceed `queue.capacity_chunks` and remains single-decoder.

## `output`, `visualization`, And `validation`

| Field | Required | Meaning |
| --- | --- | --- |
| `output.write_strategy` | yes | Use `ordered_append` for the baseline. |
| `visualization.enabled` | no | Enables post-run PNG generation. |
| `visualization.outputs` | no | `heatmap`, `stacked_waveform`, or both. |
| `visualization.max_rows` | no | Maximum CSV rows read for plotting. |
| `validation.enabled` | no | Enables exact CSV comparison. |
| `validation.reference_csv` | validation | Receiver VM path to trusted reference CSV. |
| `validation.compare_fields` | validation | Comma-separated fields to compare in order. |
| `validation.expected_packets` | no | Expected decoded packet count. |
| `validation.expected_rows` | no | Expected decoded row count. |

Exact validation compares generated output against the configured reference CSV.
Use a reference generated from the same event, mapping, and decode filters.
