# Fresh User Checklist

Use this checklist before running the public baseline on a new existing FABRIC
slice. No Python edits are required.

## 1. Install Locally

```bash
python3 -m pip install -e '.[dev,dashboard,notebook]'
```

## 2. Copy A Config

```bash
cp configs/single_decoder_baseline_template.yaml configs/my_baseline.yaml
```

Edit only YAML values in `configs/my_baseline.yaml`.

## 3. Choose Transport

Use LoomAI when running inside a configured LoomAI environment:

```yaml
transport:
  type: loom
```

Use SSH from a local workstation or standard notebook:

```yaml
transport:
  type: ssh
  username: ubuntu
  ssh_key: ~/.ssh/fabric_slice_key
  hosts:
    sender: sender.example.fabric.testbed.net
    receiver: receiver.example.fabric.testbed.net
```

Add `transport.ssh_options` for SSH config files or ProxyJump when your FABRIC
access path requires it.

## 4. Confirm VM State

Sender VM for replay:

- `python3`
- `tcpreplay`
- user-provided PCAP at `pcap_replay.sender_pcap`
- correct `network.sender_interface`

Receiver VM:

- `python3`
- `tcpdump`
- non-interactive `sudo tcpdump`
- WibViz source at `paths.wibviz_src`
- mapping CSV at `paths.mapping_csv`
- reference CSV at `validation.reference_csv` if validation is enabled
- writable `paths.receiver_root`

## 5. Run In Order

Local-only checks:

```bash
iceberg-fabric preflight --config configs/my_baseline.yaml --local-only
iceberg-fabric run --config configs/my_baseline.yaml --dry-run
```

Remote readiness preflight:

```bash
iceberg-fabric preflight --config configs/my_baseline.yaml
```

Bounded smoke test:

```bash
iceberg-fabric run --config configs/my_baseline.yaml --max-live-chunks 2
```

Full user-provided replay:

```bash
iceberg-fabric run --config configs/my_baseline.yaml
```

## Still Manual

- Creating or provisioning the FABRIC slice.
- Installing VM packages.
- Copying large PCAP, mapping, and reference files.
- Discovering management hostnames and data-plane interfaces.
- Selecting legally shareable data.
