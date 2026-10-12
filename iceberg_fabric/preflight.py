from __future__ import annotations

import importlib
import json
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass
class CheckResult:
    name: str
    status: str
    detail: str
    repair: str


def _scripts_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "scripts"


def _ensure_scripts_importable() -> None:
    scripts_dir = _scripts_dir()
    if scripts_dir.is_dir():
        sys.path.insert(0, str(scripts_dir))


def _result(name: str, status: str, detail: str, repair: str = "") -> CheckResult:
    return CheckResult(name=name, status=status, detail=detail, repair=repair)


def _import_check(module: str, package_name: str | None = None) -> CheckResult:
    label = package_name or module
    try:
        importlib.import_module(module)
    except Exception as exc:  # pragma: no cover - message matters more than subtype
        return _result("Python dependency: " + label, "FAIL", str(exc), f"Install with: python -m pip install {label}")
    return _result("Python dependency: " + label, "PASS", "import succeeded")


def _command_check(command: str, required: bool = True) -> CheckResult:
    found = shutil.which(command)
    if found:
        return _result("Local command: " + command, "PASS", found)
    status = "FAIL" if required else "WARNING"
    return _result("Local command: " + command, status, "not found", f"Install {command} or adjust transport settings.")


def _remote_exec(transport: Any, node: str, command: str, timeout: int) -> subprocess.CompletedProcess[str]:
    return transport.exec(node, command, timeout)


def _remote_check(transport: Any, node: str, name: str, command: str, timeout: int, repair: str) -> CheckResult:
    try:
        result = _remote_exec(transport, node, command, timeout)
    except Exception as exc:  # pragma: no cover - depends on user environment
        return _result(name, "FAIL", str(exc), repair)
    detail = (result.stdout or result.stderr or "").strip()
    if result.returncode == 0:
        return _result(name, "PASS", detail or "command succeeded")
    return _result(name, "FAIL", detail or f"return code {result.returncode}", repair)


def _validate_local_config(config_path: Path) -> tuple[dict[str, Any] | None, CheckResult]:
    _ensure_scripts_importable()
    try:
        run_pipeline = importlib.import_module("run_pipeline")
        config = run_pipeline.load_config(config_path)
        runner = run_pipeline.PipelineRunner(config, config_path=config_path, timeout=15)
        runner.validate_config()
        runner.dry_run_payload(emit_json=False)
    except Exception as exc:
        return None, _result(
            "Configuration schema",
            "FAIL",
            str(exc),
            "Fix the YAML fields shown in configs/single_decoder_baseline_template.yaml and rerun preflight.",
        )
    return config, _result("Configuration schema", "PASS", "config loaded and no-replay validation passed")


def _transport_checks(config: dict[str, Any], local_only: bool, timeout: int) -> list[CheckResult]:
    _ensure_scripts_importable()
    run_pipeline = importlib.import_module("run_pipeline")
    remote_transport = importlib.import_module("remote_transport")
    checks: list[CheckResult] = []
    transport_config = config.get("transport") or {"type": "loom"}
    transport_type = str(transport_config.get("type") or "loom").lower()
    slice_config = config.get("slice") or {}
    network = config.get("network") or {}
    paths = config.get("paths") or {}
    pcap = config.get("pcap_replay") or {}
    validation = config.get("validation") or {}
    sender = str(slice_config.get("sender_node") or "sender")
    receiver = str(slice_config.get("receiver_node") or "receiver")

    if transport_type in {"loom", "loomai"}:
        checks.append(_command_check("loomai"))
    elif transport_type == "ssh":
        checks.append(_command_check("ssh"))
        checks.append(_command_check(str(transport_config.get("copy_tool") or "scp")))
        if not transport_config.get("username"):
            checks.append(_result("SSH username", "FAIL", "transport.username is empty", "Set transport.username."))
        else:
            checks.append(_result("SSH username", "PASS", str(transport_config.get("username"))))
        hosts = transport_config.get("hosts") or {}
        for node in (sender, receiver):
            host = hosts.get(node) or transport_config.get(f"{node}_host")
            if host:
                checks.append(_result(f"SSH host for {node}", "PASS", str(host)))
            else:
                checks.append(_result(f"SSH host for {node}", "FAIL", "missing host", f"Set transport.hosts.{node}."))
    else:
        checks.append(_result("Transport type", "FAIL", transport_type, "Use transport.type: loom or ssh."))
        return checks

    if local_only:
        for name in (
            "Sender reachability",
            "Receiver reachability",
            "Remote tools",
            "Interfaces",
            "Capture permissions",
            "Remote input paths",
            "Remote output directory",
            "Remote disk space",
        ):
            checks.append(_result(name, "WARNING", "skipped because --local-only was used", "Run without --local-only to check remote hosts."))
        return checks

    try:
        transport = remote_transport.build_transport(transport_config, slice_name=run_pipeline.PipelineRunner(config, config_path=Path("config.yaml")).slice_name, log_file=Path("/tmp/iceberg-fabric-preflight.log"))
    except Exception as exc:
        checks.append(_result("Transport initialization", "FAIL", str(exc), "Fix the transport and slice settings."))
        return checks

    checks.append(_remote_check(transport, sender, "Sender reachability", "true", timeout, "Fix sender host, SSH key, or LoomAI slice/node name."))
    checks.append(_remote_check(transport, receiver, "Receiver reachability", "true", timeout, "Fix receiver host, SSH key, or LoomAI slice/node name."))
    checks.append(_remote_check(transport, sender, "Sender required tools", "command -v python3 >/dev/null && command -v tcpreplay >/dev/null", timeout, "Install python3 and tcpreplay on the sender."))
    checks.append(_remote_check(transport, receiver, "Receiver required tools", "command -v python3 >/dev/null && command -v tcpdump >/dev/null", timeout, "Install python3 and tcpdump on the receiver."))
    checks.append(_remote_check(transport, sender, "Sender interface", f"ip -brief addr show dev {network.get('sender_interface')}", timeout, "Set network.sender_interface to the sender data interface."))
    checks.append(_remote_check(transport, receiver, "Receiver interface", f"ip -brief addr show dev {network.get('receiver_interface')}", timeout, "Set network.receiver_interface to the receiver data interface."))
    checks.append(_remote_check(transport, receiver, "Capture permissions", "sudo -n true && sudo -n tcpdump --version >/dev/null", timeout, "Allow non-interactive sudo for tcpdump on the receiver."))
    checks.append(_remote_check(transport, sender, "Sender PCAP", f"test -r {pcap.get('sender_pcap')}", timeout, "Copy the PCAP to pcap_replay.sender_pcap on the sender."))
    checks.append(_remote_check(transport, receiver, "Receiver mapping CSV", f"test -r {paths.get('mapping_csv')}", timeout, "Copy the mapping CSV to paths.mapping_csv on the receiver."))
    if validation.get("enabled", True):
        checks.append(_remote_check(transport, receiver, "Receiver validation reference", f"test -r {validation.get('reference_csv')}", timeout, "Copy the trusted reference CSV to validation.reference_csv on the receiver."))
    checks.append(_remote_check(transport, receiver, "Receiver WibViz path", f"test -d {paths.get('wibviz_src')}", timeout, "Install or copy WibViz source to paths.wibviz_src on the receiver."))
    output_root = paths.get("output_root") or "/tmp/iceberg_fabric_runs"
    checks.append(_remote_check(transport, receiver, "Output directory permissions", f"mkdir -p {output_root} && test -w {output_root}", timeout, "Choose a writable paths.output_root on the receiver."))
    checks.append(_remote_check(transport, receiver, "Remote disk space", f"df -Pk {output_root}", timeout, "Free disk space or choose a larger output filesystem."))
    return checks


def _dependency_checks() -> list[CheckResult]:
    checks = [
        _result("Python version", "PASS" if sys.version_info >= (3, 10) else "FAIL", sys.version.split()[0], "Use Python 3.10 or newer."),
        _import_check("yaml", "PyYAML"),
        _import_check("pandas"),
        _import_check("numpy"),
        _import_check("matplotlib"),
        _import_check("scapy"),
    ]
    for optional in ("plotly", "dash", "IPython", "ipykernel", "nbformat"):
        try:
            importlib.import_module(optional)
        except Exception:
            checks.append(_result("Optional dependency: " + optional, "WARNING", "not installed", "Install with: python -m pip install '.[dashboard,notebook]'"))
        else:
            checks.append(_result("Optional dependency: " + optional, "PASS", "import succeeded"))
    return checks


def run_preflight(args: Any) -> int:
    checks: list[CheckResult] = []
    config_path = Path(args.config).expanduser()
    if not config_path.is_file():
        checks.append(_result("Configuration file", "FAIL", str(config_path), "Pass an existing YAML file with --config."))
        config = None
    else:
        checks.append(_result("Configuration file", "PASS", str(config_path)))
        config, config_check = _validate_local_config(config_path)
        checks.append(config_check)

    checks.extend(_dependency_checks())
    if config is not None:
        checks.extend(_transport_checks(config, bool(args.local_only), int(args.timeout)))

    failures = [check for check in checks if check.status == "FAIL"]
    warnings = [check for check in checks if check.status == "WARNING"]
    summary = {
        "status": "FAIL" if failures else "PASS",
        "failures": len(failures),
        "warnings": len(warnings),
        "checks": [asdict(check) for check in checks],
    }
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        for check in checks:
            print(f"{check.status}: {check.name} - {check.detail}")
            if check.status == "FAIL" and check.repair:
                print(f"  Repair: {check.repair}")
            elif check.status == "WARNING" and check.repair:
                print(f"  Next: {check.repair}")
        print(f"SUMMARY: {summary['status']} ({len(failures)} fail, {len(warnings)} warning)")
    return 1 if failures else 0
