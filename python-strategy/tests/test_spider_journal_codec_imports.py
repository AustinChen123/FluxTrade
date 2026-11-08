# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""J4 codec contract tests."""

import pytest

from spider_journal_codec_test_support import (
    Path,
    _market,
    _working,
    canonical_bytes,
    codec,
    random,
)
from spider_journal_codec_io_support import (
    _assert_source_import_boundary,
    _reorder_nested,
)


def test_seeded_200_permutations_ignore_recursive_dict_insertion_order():
    baseline = _market(
        [
            _working("first", remaining_quantity_contracts="3"),
            _working("second", remaining_quantity_contracts="4"),
        ],
        [_working("third", remaining_quantity_contracts="5")],
    )
    expected_row, expected_state = codec.encode_p3_v2_row(baseline)
    expected_bytes = canonical_bytes(expected_row)

    for seed in range(200):
        candidate = _reorder_nested(baseline, random.Random(seed))
        encoded, state = codec.encode_p3_v2_row(candidate)
        assert canonical_bytes(encoded) == expected_bytes
        assert state == expected_state


def test_reviewers_insertion_order_counterexample_and_two_run_determinism():
    first = _working("first", remaining_quantity_contracts="3")
    second = _working("second", remaining_quantity_contracts="4")
    normal = _market([first, second], [_working("third")])
    reverse = {
        key: _reorder_nested(value, random.Random(991))
        for key, value in reversed(list(normal.items()))
    }
    encoded_a, state_a = codec.encode_p3_v2_row(normal)
    encoded_b, state_b = codec.encode_p3_v2_row(reverse)
    encoded_again, state_again = codec.encode_p3_v2_row(normal)

    assert canonical_bytes(encoded_a) == canonical_bytes(encoded_b)
    assert canonical_bytes(encoded_a) == canonical_bytes(encoded_again)
    assert state_a == state_b == state_again
    assert encoded_a["order_defs"] == [
        first,
        second,
        normal["payload"]["working_orders_snapshot"][0],
    ]


def test_codec_module_has_strict_canonical_helpers_only_import_boundary():
    source = Path(codec.__file__).read_text()
    _assert_source_import_boundary(source)


@pytest.mark.parametrize(
    "injected",
    [
        "import pathlib as fs\nfs.Path('x').write_text('bad')\n",
        "import os as fs\nwriter = fs.open\n",
        "from os import open as writer\n",
        "writer = open\nwriter('x')\n",
        "dynamic = __import__('os')\n",
        "dynamic = getattr(canonical_bytes, '__globals__')\n",
        "dynamic = canonical_bytes.__globals__\n",
        "from tempfile import TemporaryFile\n",
    ],
)
def test_import_boundary_kills_alias_and_writer_capability_mutations(injected):
    source = Path(codec.__file__).read_text() + "\n" + injected
    with pytest.raises((AssertionError, SyntaxError)):
        _assert_source_import_boundary(source)


def test_codec_import_closure_does_not_load_io_or_process_modules():
    import subprocess
    import sys

    package_root = str(Path(__file__).parents[1])
    forbidden = "os pathlib shutil tempfile socket subprocess sqlite3"
    probe = (
        "import sys; sys.path.insert(0, "
        + repr(package_root)
        + "); import src.core.backtest.spider_journal_codec_v2; "
        + "bad = set("
        + repr(forbidden.split())
        + ") & set(sys.modules); "
        + "assert not bad, sorted(bad)"
    )
    subprocess.run([sys.executable, "-I", "-S", "-c", probe], check=True)
