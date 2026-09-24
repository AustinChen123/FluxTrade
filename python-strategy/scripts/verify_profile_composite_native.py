"""Offline opt-in wheel gate; never install/build or use an ambient native extension."""

import hashlib
import importlib.machinery
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import zipfile

CHILD = """
import hashlib, importlib, importlib.machinery, json, os, pathlib, sys
extract, root, member, expected = sys.argv[1:]
assert not any(n == 'fluxtrade_core' or n.startswith('fluxtrade_core.') for n in sys.modules)
sys.path[:0] = [extract, root, str(pathlib.Path(root) / 'tests')]
module = importlib.import_module('fluxtrade_core.' + pathlib.Path(member).name.split('.')[0])
native = pathlib.Path(module.__file__).resolve()
assert native.is_relative_to(pathlib.Path(extract).resolve())
assert any(str(native).endswith(s) for s in importlib.machinery.EXTENSION_SUFFIXES)
assert hashlib.sha256(native.read_bytes()).hexdigest() == expected
before = (module, native, tuple(sys.path))
os.environ['FLUXTRADE_COMPOSITE_NATIVE_GATE'] = '1'
for key in ('PYTEST_ADDOPTS', 'PYTEST_PLUGINS'):
    os.environ.pop(key, None)
os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
import pytest
class Audit:
    collected = passed = rejected = deselected = 0
    def pytest_collection_finish(self, session):
        self.collected = len(session.items)
    def pytest_deselected(self, items):
        self.deselected += len(items)
    def pytest_runtest_logreport(self, report):
        self.rejected += int(report.failed or report.skipped)
        self.passed += int(report.when == 'call' and report.passed)
audit = Audit()
code = pytest.main(['-q', '-o', 'addopts=', '--import-mode=importlib', str(pathlib.Path(root) / 'tests/test_profile_composite_native.py')], plugins=[audit])
assert code == 0 and (audit.collected, audit.passed, audit.rejected, audit.deselected) == (9, 9, 0, 0)
assert sys.modules[module.__name__] is before[0]
assert pathlib.Path(module.__file__).resolve() == before[1]
assert hashlib.sha256(native.read_bytes()).hexdigest() == expected
assert sys.path[:3] == list(before[2][:3])
print(json.dumps(dict(native_path=str(native), native_sha256=expected, exit_code=code)))
raise SystemExit(code)
"""


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("expected absolute wheel path")
    wheel = Path(sys.argv[1])
    if not wheel.is_absolute() or not wheel.is_file() or wheel.suffix != ".whl":
        raise SystemExit("invalid wheel path")
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="profile-native-") as temporary:
        with zipfile.ZipFile(wheel) as archive:
            members = archive.namelist()
            assert all(
                not Path(n).is_absolute() and ".." not in Path(n).parts for n in members
            )
            natives = [
                n
                for n in members
                if n.startswith("fluxtrade_core/")
                and any(n.endswith(s) for s in importlib.machinery.EXTENSION_SUFFIXES)
            ]
            assert len(natives) == 1
            digest = hashlib.sha256(archive.read(natives[0])).hexdigest()
            archive.extractall(temporary)
        print(
            json.dumps(
                dict(
                    wheel_path=str(wheel),
                    wheel_sha256=hashlib.sha256(wheel.read_bytes()).hexdigest(),
                )
            ),
            flush=True,
        )
        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                CHILD,
                temporary,
                str(root),
                natives[0],
                digest,
            ],
            check=False,
            timeout=180,
        )
        return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
