#!/usr/bin/env python3
from __future__ import annotations

import shlex
import subprocess
from pathlib import Path
from typing import Any, Protocol


class TransportConfigError(ValueError):
    pass


class RemoteTransport(Protocol):
    transport_type: str

    def exec(self, node: str, command: str, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
        ...

    def copy_to(self, node: str, local_path: Path, remote_path: str, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
        ...

    def copy_from(self, node: str, remote_path: str, local_path: Path, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
        ...

    def describe(self) -> dict[str, Any]:
        ...


def shell_join(args: list[str]) -> str:
    return " ".join(shlex.quote(str(arg)) for arg in args)


def run_command(args: list[str], log_file: Path, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    started = _now_iso()
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as log:
        log.write(f"\n### {started}\n")
        log.write("COMMAND: " + shell_join(args) + "\n")
    try:
        result = subprocess.run(args, text=True, capture_output=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as error:
        stdout = _timeout_text(error.stdout)
        stderr = _timeout_text(error.stderr)
        timeout_message = f"TIMEOUT after {timeout}s"
        result = subprocess.CompletedProcess(
            args=args,
            returncode=124,
            stdout=stdout,
            stderr=(stderr + "\n" if stderr else "") + timeout_message,
        )
    with log_file.open("a", encoding="utf-8") as log:
        log.write(f"EXIT: {result.returncode} END: {_now_iso()}\n")
        if result.stdout:
            log.write("STDOUT:\n" + result.stdout + "\n")
        if result.stderr:
            log.write("STDERR:\n" + result.stderr + "\n")
    return result


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _timeout_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _as_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return shlex.split(value)
    if isinstance(value, list):
        return [str(item) for item in value]
    raise TransportConfigError("ssh_options must be a string or list")


class LoomTransport:
    transport_type = "loom"

    def __init__(self, *, slice_name: str, log_file: Path):
        if not slice_name:
            raise TransportConfigError("slice.name is required for Loom transport")
        self.slice_name = slice_name
        self.log_file = log_file

    def exec(self, node: str, command: str, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
        remote = "bash -lc " + shlex.quote(command)
        return run_command(["loomai", "exec", self.slice_name, remote, "--nodes", node], self.log_file, timeout)

    def copy_to(self, node: str, local_path: Path, remote_path: str, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
        return run_command(
            ["loomai", "scp", self.slice_name, node, str(local_path), remote_path],
            self.log_file,
            timeout,
        )

    def copy_from(self, node: str, remote_path: str, local_path: Path, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        return run_command(
            ["loomai", "scp", self.slice_name, node, "--download", remote_path, str(local_path)],
            self.log_file,
            timeout,
        )

    def describe(self) -> dict[str, Any]:
        return {"type": self.transport_type, "slice_name": self.slice_name}


class SSHTransport:
    transport_type = "ssh"

    def __init__(
        self,
        *,
        username: str,
        hosts: dict[str, str],
        log_file: Path,
        ssh_key: str | None = None,
        ssh_options: list[str] | None = None,
        copy_tool: str = "scp",
    ):
        if not username:
            raise TransportConfigError("transport.username is required for SSH transport")
        if copy_tool not in {"scp", "rsync"}:
            raise TransportConfigError("transport.copy_tool must be scp or rsync")
        self.username = username
        self.hosts = {node: host for node, host in hosts.items() if host}
        self.log_file = log_file
        self.ssh_key = str(Path(ssh_key).expanduser()) if ssh_key else ""
        self.ssh_options = ssh_options or []
        self.copy_tool = copy_tool

    @classmethod
    def from_config(cls, config: dict[str, Any], *, log_file: Path) -> SSHTransport:
        hosts = dict(config.get("hosts") or {})
        for key, value in config.items():
            if key.endswith("_host") and value:
                hosts[key[: -len("_host")]] = str(value)
        return cls(
            username=str(config.get("username") or ""),
            hosts={str(node): str(host) for node, host in hosts.items()},
            log_file=log_file,
            ssh_key=str(config.get("ssh_key") or "") or None,
            ssh_options=_as_list(config.get("ssh_options")),
            copy_tool=str(config.get("copy_tool") or "scp"),
        )

    def exec(self, node: str, command: str, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
        remote = "bash -lc " + shlex.quote(command)
        return run_command(["ssh", *self._ssh_args(), self._target(node), remote], self.log_file, timeout)

    def copy_to(self, node: str, local_path: Path, remote_path: str, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
        if self.copy_tool == "rsync":
            return run_command(
                [
                    "rsync",
                    "-av",
                    "-e",
                    shell_join(["ssh", *self._ssh_args()]),
                    str(local_path),
                    self._remote_spec(node, remote_path),
                ],
                self.log_file,
                timeout,
            )
        return run_command(
            ["scp", *self._ssh_args(), str(local_path), self._remote_spec(node, remote_path)],
            self.log_file,
            timeout,
        )

    def copy_from(self, node: str, remote_path: str, local_path: Path, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if self.copy_tool == "rsync":
            return run_command(
                [
                    "rsync",
                    "-av",
                    "-e",
                    shell_join(["ssh", *self._ssh_args()]),
                    self._remote_spec(node, remote_path),
                    str(local_path),
                ],
                self.log_file,
                timeout,
            )
        return run_command(
            ["scp", *self._ssh_args(), self._remote_spec(node, remote_path), str(local_path)],
            self.log_file,
            timeout,
        )

    def describe(self) -> dict[str, Any]:
        return {
            "type": self.transport_type,
            "username": self.username,
            "hosts": dict(sorted(self.hosts.items())),
            "ssh_key": self.ssh_key or None,
            "ssh_options": self.ssh_options,
            "copy_tool": self.copy_tool,
        }

    def _ssh_args(self) -> list[str]:
        args = list(self.ssh_options)
        if self.ssh_key:
            args.extend(["-i", self.ssh_key])
        return args

    def _target(self, node: str) -> str:
        host = self.hosts.get(node)
        if not host:
            raise TransportConfigError(f"transport.hosts.{node} or transport.{node}_host is required")
        return f"{self.username}@{host}"

    def _remote_spec(self, node: str, remote_path: str) -> str:
        return f"{self._target(node)}:{shlex.quote(remote_path)}"


def build_transport(config: dict[str, Any] | None, *, slice_name: str, log_file: Path) -> RemoteTransport:
    transport_config = config or {"type": "loom"}
    transport_type = str(transport_config.get("type") or "loom").strip().lower()
    if transport_type in {"loom", "loomai"}:
        return LoomTransport(slice_name=slice_name, log_file=log_file)
    if transport_type == "ssh":
        return SSHTransport.from_config(transport_config, log_file=log_file)
    raise TransportConfigError(f"unsupported transport.type: {transport_type}")
