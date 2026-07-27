from __future__ import annotations

import mimetypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Type
from urllib.parse import unquote, urlsplit

from src.control_plane.app import ControlPlaneApp


def make_handler(
    app: ControlPlaneApp,
    *,
    static_dir: str | Path | None = None,
) -> Type[BaseHTTPRequestHandler]:
    """Build a stdlib HTTP handler for the framework-neutral app."""
    static_root = Path(static_dir).resolve() if static_dir is not None else None

    class ControlPlaneHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self._serve_static():
                return
            self._handle()

        def do_POST(self) -> None:
            self._handle()

        def _handle(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length > 0 else None
            response = app.handle(
                self.command,
                self.path,
                body,
                dict(self.headers.items()),
            )
            encoded = response.json().encode("utf-8")
            self.send_response(response.status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            for name, value in response.headers:
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(encoded)

        def _serve_static(self) -> bool:
            if static_root is None:
                return False
            request_path = unquote(urlsplit(self.path).path)
            if request_path in {"/", "/index.html"}:
                relative_path = Path("index.html")
            elif request_path.startswith("/assets/"):
                relative_path = Path(request_path.removeprefix("/"))
            else:
                return False
            candidate = (static_root / relative_path).resolve()
            if (
                not candidate.is_relative_to(static_root)
                or not candidate.is_file()
            ):
                self.send_error(404)
                return True
            content = candidate.read_bytes()
            content_type, _ = mimetypes.guess_type(candidate.name)
            self.send_response(200)
            self.send_header(
                "Content-Type",
                content_type or "application/octet-stream",
            )
            self.send_header("Content-Length", str(len(content)))
            self.send_header(
                "Cache-Control",
                "public, max-age=31536000, immutable"
                if request_path.startswith("/assets/")
                else "no-cache",
            )
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'self'; "
                "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
                "connect-src 'self'; object-src 'none'; base-uri 'none'; "
                "frame-ancestors 'none'",
            )
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(content)
            return True

        def log_message(self, format: str, *args) -> None:
            return

    return ControlPlaneHandler


def serve(
    app: ControlPlaneApp,
    host: str = "127.0.0.1",
    port: int = 8080,
    *,
    static_dir: str | Path | None = None,
) -> None:
    server = ThreadingHTTPServer(
        (host, port),
        make_handler(app, static_dir=static_dir),
    )
    server.serve_forever()
