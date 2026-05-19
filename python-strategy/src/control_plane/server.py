from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Type

from src.control_plane.app import ControlPlaneApp


def make_handler(app: ControlPlaneApp) -> Type[BaseHTTPRequestHandler]:
    """Build a stdlib HTTP handler for the framework-neutral app."""

    class ControlPlaneHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._handle()

        def do_POST(self) -> None:
            self._handle()

        def _handle(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length > 0 else None
            response = app.handle(self.command, self.path, body)
            encoded = response.json().encode("utf-8")
            self.send_response(response.status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args) -> None:
            return

    return ControlPlaneHandler


def serve(app: ControlPlaneApp, host: str = "127.0.0.1", port: int = 8080) -> None:
    server = ThreadingHTTPServer((host, port), make_handler(app))
    server.serve_forever()
