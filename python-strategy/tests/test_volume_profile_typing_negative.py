"""Offline checker acceptance of the non-constructible native result contract."""

import importlib.util
import json
from pathlib import Path
import shutil
import subprocess


def test_result_constructor_is_statically_uninhabitable(tmp_path: Path) -> None:
    node = shutil.which("node")
    assert node is not None, "installed Node required"
    package = importlib.util.find_spec("pyright")
    assert package is not None and package.origin is not None, (
        "installed Pyright required"
    )
    script = Path(package.origin).parent / "dist/dist/pyright.js"
    assert script.is_file(), "installed Pyright JavaScript required"
    runtime = tmp_path / "runtime"
    source = runtime / "fluxtrade_core"
    source.mkdir(parents=True)
    (source / "__init__.py").write_text("", encoding="utf-8")
    probe = tmp_path / "probe.py"
    probe.write_text(
        "from fluxtrade_core import VolumeProfileMergeResult, merge_volume_profiles\n"
        "valid: VolumeProfileMergeResult = merge_volume_profiles("
        "'BINANCE:BTCUSDT-SPOT', '0', '10', 'USDT', [(0, 86400000, [])], '10')\n"
        "VolumeProfileMergeResult()\n"
        "VolumeProfileMergeResult(None)\n",
        encoding="utf-8",
    )
    config = tmp_path / "pyrightconfig.json"
    config.write_text(
        json.dumps(
            {
                "stubPath": str(Path(__file__).resolve().parents[1] / "typings"),
                "extraPaths": [str(runtime)],
                "include": [probe.name],
                "typeshedPath": str(script.parent / "typeshed-fallback"),
                "pythonVersion": "3.13",
                "typeCheckingMode": "basic",
            }
        ),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [node, str(script), "--project", str(config), "--outputjson"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 1, completed.stderr
    report = json.loads(completed.stdout)
    diagnostics = report["generalDiagnostics"]
    assert report["summary"]["errorCount"] == 2
    assert len(diagnostics) == 2
    assert [item["range"]["start"]["line"] for item in diagnostics] == [2, 3]
    assert all(
        item["severity"] == "error" and Path(item["file"]) == probe
        for item in diagnostics
    )
    assert diagnostics[0]["rule"] == "reportCallIssue"
    assert "_not_constructible" in diagnostics[0]["message"]
    assert "missing" in diagnostics[0]["message"].lower()
    assert diagnostics[1]["rule"] == "reportArgumentType"
    assert "Never" in diagnostics[1]["message"] and "None" in diagnostics[1]["message"]
