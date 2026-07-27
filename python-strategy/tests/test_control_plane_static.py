import json
from http.server import ThreadingHTTPServer
from threading import Thread
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

from src.control_plane.app import ControlPlaneApp
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.server import make_handler


@pytest.fixture
def static_control_plane(tmp_path):
    static_dir = tmp_path / "frontend"
    assets_dir = static_dir / "assets"
    assets_dir.mkdir(parents=True)
    (static_dir / "index.html").write_text(
        '<script type="module" src="/assets/app.js"></script>',
        encoding="utf-8",
    )
    (assets_dir / "app.js").write_text(
        'document.body.dataset.ready = "true";',
        encoding="utf-8",
    )
    (tmp_path / "secret.txt").write_text("not public", encoding="utf-8")
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True))
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        make_handler(app, static_dir=static_dir),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_control_plane_serves_frontend_and_preserves_api(static_control_plane):
    with urlopen(f"{static_control_plane}/", timeout=2) as response:
        assert response.headers["Content-Type"] == "text/html"
        assert response.headers["Cache-Control"] == "no-cache"
        assert response.headers["Content-Security-Policy"] == (
            "default-src 'self'; script-src 'self'; "
            "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
            "connect-src 'self'; object-src 'none'; base-uri 'none'; "
            "frame-ancestors 'none'"
        )
        assert response.headers["Referrer-Policy"] == "no-referrer"
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert b"/assets/app.js" in response.read()

    with urlopen(f"{static_control_plane}/assets/app.js", timeout=2) as response:
        assert "javascript" in response.headers["Content-Type"]
        assert response.headers["Cache-Control"] == (
            "public, max-age=31536000, immutable"
        )

    with urlopen(f"{static_control_plane}/health", timeout=2) as response:
        assert json.load(response) == {"status": "ok"}


def test_control_plane_static_files_reject_path_traversal(static_control_plane):
    with pytest.raises(HTTPError) as exc_info:
        urlopen(
            f"{static_control_plane}/assets/%2e%2e/%2e%2e/secret.txt",
            timeout=2,
        )

    assert exc_info.value.code == 404
