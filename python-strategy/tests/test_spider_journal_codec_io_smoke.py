# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""P3 v2 wire helper smoke with an independent reader round trip."""

from spider_journal_codec_test_support import (
    canonical_bytes,
)
from spider_journal_codec_io_support import (
    _encode_rows,
    _generated_rows,
    _wire_read,
)


def test_generated_rows_round_trip_through_j3_reader(tmp_path):
    sources = _generated_rows()
    encoded, state = _encode_rows(sources)
    actual = _wire_read(encoded, tmp_path)
    assert len(state) == 4
    assert [canonical_bytes(row) + b"\n" for row in actual] == [
        canonical_bytes(row) + b"\n" for row in sources
    ]
