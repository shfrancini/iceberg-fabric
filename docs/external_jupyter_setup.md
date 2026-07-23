# External / Local Jupyter Setup

This guide is for running `wib_fabric_live_demo` outside LoomAI, such as from a laptop, desktop, or standard FABRIC notebook server. The portability path is `transport.type: ssh`.

## Honest Status

- LoomAI transport is currently validated.
- SSH transport is implemented and dry-run validated.
- A true external-Jupyter smoke test still requires real VM host/key details, a reachable FABRIC slice, and prepared VM-side input files.


## External Jupyter SSH Setup Helper

Use this helper if you are not already comfortable with SSH. The LoomAI path is validated. The SSH path is implemented and dry-run validated, but a true external SSH smoke test still requires real VM hosts, keys, and staged VM files.

### 1. What You Need

| Item | Why it matters |
| --- | --- |
| Local machine or external Jupyter environment | Runs this notebook and `scripts/run_pipeline.py`. |
| FABRIC account/access | Needed to use the slice VMs. |
| Active FABRIC slice with sender and receiver VMs | The runner does not create the slice yet. |
| Sender/receiver management IPs or hostnames | Used by SSH; these are not detector-net IPs. |
| SSH private key accepted by FABRIC | Used to log in to the VMs. |
| Package directory copied/cloned locally | Contains this notebook, configs, and scripts. |
| Required files already staged on VMs | PCAP on sender; mapping/reference/WIBViz files on receiver. |

### 2. Check Whether You Already Have An SSH Key

From a terminal on the local/Jupyter machine:

```bash
ls ~/.ssh
ls -la ~/.ssh
```

Common key pairs look like this:

| Private key | Public key |
| --- | --- |
| `~/.ssh/id_ed25519` | `~/.ssh/id_ed25519.pub` |
| `~/.ssh/id_rsa` | `~/.ssh/id_rsa.pub` |
| `~/.ssh/fabric_wibviz` | `~/.ssh/fabric_wibviz.pub` |

The private key has no `.pub` suffix. Do not share the private key.

### 3. Generate A New SSH Key If Needed

If you do not already have a FABRIC-accepted key, generate one:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/fabric_wibviz -C "fabric-wibviz"
```

This creates two files. `~/.ssh/fabric_wibviz` is the private key and stays on your local/Jupyter machine. `~/.ssh/fabric_wibviz.pub` is the public key and is what you register or share for access.

### 4. Register The Public Key For FABRIC Use

The exact registration step depends on your FABRIC/project setup. Add the public key wherever FABRIC SSH keys are managed, or use the key your project already provides.

Print the public key with:

```bash
cat ~/.ssh/fabric_wibviz.pub
```

Do not paste or upload `~/.ssh/fabric_wibviz`; that is the private key.

### 5. Find Sender And Receiver Management Hosts

Use the management IPs, DNS names, or SSH config aliases for the VMs. Do not use detector/data-plane addresses such as `192.168.1.1` or `192.168.1.2` for SSH unless your environment explicitly routes them for management.

```bash
SENDER_HOST=<sender-management-host-or-ip>
RECEIVER_HOST=<receiver-management-host-or-ip>
```

### 6. Test Raw SSH Access Manually

For the packaged Ubuntu examples:

```bash
ssh -i ~/.ssh/fabric_wibviz ubuntu@$SENDER_HOST hostname
ssh -i ~/.ssh/fabric_wibviz ubuntu@$RECEIVER_HOST hostname
```

If your site requires a bastion or ProxyJump:

```bash
ssh -i ~/.ssh/fabric_wibviz -J <bastion-user>@<bastion-host> ubuntu@$SENDER_HOST hostname
```

If these commands fail, the notebook pipeline will not work yet. Fix raw SSH first.

### 7. Create The Package SSH Config

Copy the template:

```bash
cp configs/event_4245_replay_ssh_template.yaml configs/event_4245_replay_ssh.yaml
```

Edit the copied file. The SSH user field is named `username` in this package config.

Example:

```yaml
transport:
  type: ssh
  username: ubuntu
  sender_host: 198.51.100.10
  receiver_host: 198.51.100.11
  ssh_key: ~/.ssh/fabric_wibviz
  ssh_options:
    - -o
    - BatchMode=yes
    - -o
    - StrictHostKeyChecking=accept-new
  copy_tool: scp
```

If you need ProxyJump in the package config, add it to `ssh_options`:

```yaml
ssh_options:
  - -J
  - <bastion-user>@<bastion-host>
  - -o
  - BatchMode=yes
```

Also verify the remote file paths in the same YAML: `pcap_replay.sender_pcap` on the sender VM, `paths.mapping_csv` on the receiver VM, and optional `validation.reference_csv` on the receiver VM.

### 8. Test Package-Level SSH Dry-Run

Dry-run parses the config and shows planned commands. It does not contact VMs or run replay.

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --dry-run
```

### 9. Run The First SSH Smoke Test

Only after dry-run and raw SSH access work, run the two-chunk smoke test:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --max-live-chunks 2
```

### 10. SSH Setup Troubleshooting

| Symptom | Likely cause | First fix |
| --- | --- | --- |
| `Permission denied (publickey)` | Wrong key/user or public key not registered | Check `username`, `ssh_key`, and FABRIC/project key registration. |
| `Connection timed out` | Wrong host, VPN/bastion/security-group issue | Verify management host and any required bastion/ProxyJump. |
| `No route to host` | Local environment cannot reach the FABRIC management network | Use the FABRIC SSH config, VPN, or correct bastion path. |
| `tcpreplay: not found` | Sender VM dependency missing | Install `tcpreplay` on the sender VM. |
| `tcpdump: not found` | Receiver VM dependency missing | Install `tcpdump` on the receiver VM. |
| PCAP not found | Source PCAP missing on sender | Stage the PCAP at `pcap_replay.sender_pcap`. |
| Mapping CSV not found | Mapping CSV missing on receiver | Stage the mapping CSV at `paths.mapping_csv`. |

## Minimum Test Path

1. Copy this package to the local machine or clone the repository.
2. Start Jupyter from the `wib_fabric_live_demo` directory, or set `package_root_override` in `00_RUN_DEMO.ipynb`.
3. Install local Python dependencies:

```bash
python3 -m pip install pyyaml pandas matplotlib
```

4. Confirm local SSH tools exist:

```bash
ssh -V
scp -V 2>/dev/null || true
```

5. Copy the replay SSH template:

```bash
cp configs/event_4245_replay_ssh_template.yaml configs/event_4245_replay_ssh.yaml
```

6. Edit the copied config:

```yaml
transport:
  type: ssh
  username: ubuntu
  sender_host: <sender-management-ip-or-ssh-config-alias>
  receiver_host: <receiver-management-ip-or-ssh-config-alias>
  ssh_key: ~/.ssh/fabric_slice_key
  ssh_options:
    - -o
    - BatchMode=yes
```

If FABRIC generated an SSH config, use it:

```yaml
ssh_options:
  - -F
  - /path/to/fabric_ssh_config
  - -o
  - BatchMode=yes
```

If you need a bastion directly, use `ProxyJump`/`-J`:

```yaml
ssh_options:
  - -J
  - <bastion-host>
  - -o
  - BatchMode=yes
```

7. Confirm remote VM paths in the same config:

```yaml
paths:
  output_root: /tmp/wib_fabric_live_demo      # local Jupyter machine
  receiver_root: /home/ubuntu                  # receiver VM
  sender_root: /home/ubuntu                    # sender VM
  wibviz_src: /home/ubuntu/wibviz_graph/src    # receiver VM
  mapping_csv: /home/ubuntu/wibviz_inputs/event-4245-channel-map.csv
pcap_replay:
  sender_pcap: /home/ubuntu/wib-replay/pcap/icecosmich_run82134054_sub001_evt004245.pcap
validation:
  reference_csv: /home/ubuntu/wibviz_inputs/4245.csv
```

8. Run dry-run. This does not contact VMs:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --dry-run
```

9. After dry-run looks correct and SSH/VM files are confirmed, run a 2-chunk smoke test:

```bash
python3 scripts/run_pipeline.py --config configs/event_4245_replay_ssh.yaml --max-live-chunks 2
```

10. Inspect local outputs under `/tmp/wib_fabric_live_demo/runs/<run_id>/` unless you used `--output-root`.

## Local vs Remote Path Rules

- `paths.output_root` is local to the machine running Jupyter and `scripts/run_pipeline.py`.
- `paths.receiver_root`, `paths.wibviz_src`, `paths.mapping_csv`, and `validation.reference_csv` are receiver VM paths.
- `paths.sender_root` and `pcap_replay.sender_pcap` are sender VM paths.
- Remote run products are created under `<receiver_root>/wibviz_runs/...` and `<receiver_root>/wibviz_outputs/...`.
- Local visualization products are downloaded/rendered under `<output_root>/runs/<run_id>/`.

## What SSH Transport Does Not Discover Yet

- It does not discover sender/receiver VM management IPs.
- It does not create SSH keys or FABRIC bastion config.
- It does not install VM packages.
- It does not upload PCAPs, mapping CSVs, reference CSVs, or WIBViz source.
- It does not validate actual VM reachability during dry-run.

## Live NIC From External Jupyter

Copy the live template instead:

```bash
cp configs/live_nic_ssh_template.yaml configs/live_nic_ssh.yaml
python3 scripts/run_pipeline.py --config configs/live_nic_ssh.yaml --dry-run
```

Then run bounded capture only after real receiver details are correct:

```bash
python3 scripts/run_pipeline.py --config configs/live_nic_ssh.yaml --max-live-chunks 3
```
