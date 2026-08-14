import argparse
import sys
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


DEFAULT_MAX_BYTES = 256 * 1024
MAX_REQUEST_BYTES = 1024 * 1024


@dataclass(frozen=True)
class FileChunk:
    data: bytes
    next_offset: int
    file_size: int
    file_mtime_ns: int
    reset: bool = False


def read_complete_chunk(csv_path, offset=0, max_bytes=DEFAULT_MAX_BYTES):
    """Read complete CSV lines beginning at a byte offset.

    The writer may be appending while this function runs. Returning data only
    through the final newline prevents clients from parsing a partial row.
    """
    csv_path = Path(csv_path)
    if offset < 0:
        raise ValueError("offset must be non-negative")
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    if not csv_path.is_file():
        return None

    stat = csv_path.stat()
    reset = offset > stat.st_size
    if reset:
        offset = 0

    with csv_path.open("rb") as csv_file:
        csv_file.seek(offset)
        data = csv_file.read(max_bytes)

    final_newline = data.rfind(b"\n")
    if final_newline < 0:
        data = b""
    else:
        data = data[: final_newline + 1]

    return FileChunk(
        data=data,
        next_offset=offset + len(data),
        file_size=stat.st_size,
        file_mtime_ns=stat.st_mtime_ns,
        reset=reset,
    )


def create_handler(csv_path, default_max_bytes=DEFAULT_MAX_BYTES):
    csv_path = Path(csv_path)

    class DeltaHandler(BaseHTTPRequestHandler):
        def _request_integer(self, query, name, default):
            raw_value = query.get(name, [None])[0]
            if raw_value is None and name == "offset":
                raw_value = self.headers.get("X-Byte-Offset")
            if raw_value is None:
                return default
            return int(raw_value)

        def _send_chunk_headers(self, status, chunk=None):
            self.send_response(status)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header(
                "Access-Control-Expose-Headers",
                (
                    "X-Next-Offset, X-File-Size, X-File-Mtime-Ns, "
                    "X-Server-Time-Ns, X-Stream-Reset"
                ),
            )
            self.send_header("X-Server-Time-Ns", str(time.time_ns()))
            if chunk is not None:
                self.send_header("X-Next-Offset", str(chunk.next_offset))
                self.send_header("X-File-Size", str(chunk.file_size))
                self.send_header("X-File-Mtime-Ns", str(chunk.file_mtime_ns))
                self.send_header("X-Stream-Reset", "1" if chunk.reset else "0")

        def do_GET(self):
            try:
                query = parse_qs(urlparse(self.path).query)
                offset = self._request_integer(query, "offset", 0)
                max_bytes = self._request_integer(
                    query,
                    "max_bytes",
                    default_max_bytes,
                )
                max_bytes = min(max_bytes, MAX_REQUEST_BYTES)
                chunk = read_complete_chunk(csv_path, offset, max_bytes)
            except (OSError, TypeError, ValueError) as error:
                message = f"Invalid delta request: {error}\n".encode("utf-8")
                self._send_chunk_headers(400)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(message)))
                self.end_headers()
                self.wfile.write(message)
                return

            if chunk is None or not chunk.data:
                self._send_chunk_headers(204, chunk)
                self.end_headers()
                return

            self._send_chunk_headers(200, chunk)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Length", str(len(chunk.data)))
            self.end_headers()
            self.wfile.write(chunk.data)

        def do_HEAD(self):
            try:
                chunk = read_complete_chunk(csv_path, 0, 1)
            except OSError:
                chunk = None
            self._send_chunk_headers(200 if chunk is not None else 204, chunk)
            self.end_headers()

    return DeltaHandler


def build_parser():
    parser = argparse.ArgumentParser(
        description="Serve newly appended complete CSV rows by byte offset."
    )
    parser.add_argument("--csv", required=True, help="Growing exporter CSV.")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address.")
    parser.add_argument("--port", type=int, default=8063, help="Listen port.")
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BYTES,
        help="Maximum response body size (default: 262144).",
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.max_bytes < 1 or args.max_bytes > MAX_REQUEST_BYTES:
        raise SystemExit(
            f"--max-bytes must be between 1 and {MAX_REQUEST_BYTES}"
        )

    handler = create_handler(args.csv, args.max_bytes)
    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"Delta server: http://{args.host}:{args.port}")
    print(f"Monitoring CSV: {Path(args.csv).expanduser().resolve()}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

