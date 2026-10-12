# FABRIC Transport

The runner reaches VMs through a transport abstraction. The pipeline behavior is
the same for LoomAI and SSH: execute remote commands, copy helper scripts, copy
artifacts back, and never modify the slice topology.

## LoomAI

Use this when the `loomai` CLI is available and configured:

```yaml
transport:
  type: loom
slice:
  name: your-fabric-slice
  sender_node: sender
  receiver_node: receiver
```

Run:

```bash
iceberg-fabric preflight --config configs/my_baseline.yaml
iceberg-fabric run --config configs/my_baseline.yaml --dry-run
```

## SSH

Use this from external notebooks or workstations:

```yaml
transport:
  type: ssh
  username: ubuntu
  ssh_key: ~/.ssh/fabric_slice_key
  ssh_options:
    - -o
    - BatchMode=yes
    - -o
    - StrictHostKeyChecking=accept-new
  copy_tool: scp
  hosts:
    sender: sender.example.fabric.testbed.net
    receiver: receiver.example.fabric.testbed.net
```

For nonstandard logical names, make the host keys match `slice.sender_node` and
`slice.receiver_node`.

## What Transport Does Not Do

- It does not create, submit, renew, or delete FABRIC slices.
- It does not discover management IPs or data-plane interface names.
- It does not install `tcpdump`, `tcpreplay`, Python packages, or WibViz.
- It does not upload large PCAP or reference files.
