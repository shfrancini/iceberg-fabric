# Standard FABRIC SSH Transport

`dune_fabric_live_demo` now separates pipeline logic from the mechanism used to reach FABRIC VMs. The runner talks to a small transport interface:

- `exec(node, command, timeout=None)`
- `copy_to(node, local_path, remote_path)`
- `copy_from(node, remote_path, local_path)`

The shared capture, replay, decode, validation, manifest, and visualization pipeline is unchanged. Only the backend that executes remote commands and moves files changes.

## LoomAI Transport

The LoomAI backend is the currently tested path and preserves the original behavior. It resolves logical node names through the configured LoomAI slice and uses `loomai exec` / `loomai scp` internally.

```yaml
transport:
  type: loom

slice:
  name: dune-udp-demo-rebuild-2
  sender_node: sender
  receiver_node: receiver
```

Run a dry-run from LoomAI Jupyter:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml --dry-run
```

Run the replay only when the dry-run plan and remote paths are correct:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay.yaml
```

## SSH Transport

The SSH backend is the portability path for standard FABRIC notebooks or local Jupyter sessions. It uses the logical node names from `slice.sender_node` and `slice.receiver_node`, then maps those names to SSH hosts in the `transport` block.

```yaml
transport:
  type: ssh
  username: ubuntu
  sender_host: replace-with-sender-management-ip-or-dns
  receiver_host: replace-with-receiver-management-ip-or-dns
  ssh_key: ~/.ssh/fabric_slice_key
  ssh_options:
    - -o
    - BatchMode=yes
    - -o
    - StrictHostKeyChecking=accept-new
  copy_tool: scp
```

For non-`sender` / `receiver` logical node names, use an explicit map:

```yaml
transport:
  type: ssh
  username: ubuntu
  hosts:
    tx: 192.0.2.10
    rx: 192.0.2.11
```

The SSH backend also accepts `copy_tool: rsync` if `rsync` is available locally and on the target VM.

Start from the templates:

```bash
cp configs/event_4245_replay_ssh_template.yaml configs/event_4245_replay_ssh.yaml
cp configs/live_nic_ssh_template.yaml configs/live_nic_ssh.yaml
```

Fill in the hostnames/IPs and key path, then dry-run:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --dry-run
python3 scripts/run_pipeline.py --config configs/live_nic_ssh.yaml --dry-run
```

## VM Requirements

The transport only changes how the runner reaches nodes. The VM-side contract is still the same:

- The receiver must have `tcpdump`, `python3`, the WIBViz source tree, and the configured mapping CSV.
- The sender must have `tcpreplay` and the configured source PCAP for `pcap_replay`.
- The SSH user must be able to run the configured `sudo tcpdump` command non-interactively.
- The local notebook environment must have network reachability to the VM management addresses, including any FABRIC bastion SSH config required by the slice.

## Current Portability Gaps

SSH transport removes the LoomAI CLI dependency from pipeline execution, but full standard FABRIC portability still depends on setup outside this runner:

- Discovering sender/receiver management IPs is still manual in the SSH templates.
- VM provisioning and installation of WIBViz, `tcpdump`, `tcpreplay`, input PCAPs, and mapping/reference CSVs are not automated here.
- Interface names and expected data-plane IPs must still match the user-created FABRIC topology.
- External/local Jupyter users may need an SSH config or ProxyJump through FABRIC bastions.
