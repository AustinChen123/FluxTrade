from __future__ import annotations

import os

from src.control_plane import BacktestJobExecutor, ControlPlaneApp
from src.control_plane.server import serve


def main() -> None:
    host = os.getenv("CONTROL_PLANE_HOST", "127.0.0.1")
    port = int(os.getenv("CONTROL_PLANE_PORT", "8080"))
    app = ControlPlaneApp(BacktestJobExecutor())
    serve(app, host=host, port=port)


if __name__ == "__main__":
    main()
