from __future__ import annotations

from types import MethodType
from typing import cast
from unittest.mock import Mock

import pytest

from src.control_plane.backtest_result_http_contract import (
    IndexCursor,
    InvalidBacktestResultHttpRequest,
    encode_result_cursor,
    parse_result_query,
)
from src.control_plane.jobs import InMemoryJobStore
from src.control_plane.main import build_control_plane_app
from src.control_plane.models import JobRecord


class _CountingStore(InMemoryJobStore):
    def __init__(self) -> None:
        super().__init__()
        self.get_calls = 0

    def get(self, job_id: str) -> JobRecord | None:
        self.get_calls += 1
        return super().get(job_id)


def test_default_reader_composition_is_lazy_and_scoped_per_app() -> None:
    factory = Mock(side_effect=AssertionError("composition must not query SQL"))
    store = _CountingStore()

    def build():
        return build_control_plane_app(
            redis_client=Mock(),
            db_session_factory=factory,
            job_store=store,
            api_key="test-key",
            readiness_probe=lambda: None,
            strategy_loader=lambda: {},
        )

    first, second = build(), build()
    try:
        left = first.backtest_results_query_service
        right = second.backtest_results_query_service
        assert left is not None and right is not None and left is not right
        assert left._session_factory is factory and right._session_factory is factory
        assert left._job_lookup is not None and right._job_lookup is not None
        assert cast(MethodType, left._job_lookup).__self__ is store
        assert cast(MethodType, right._job_lookup).__self__ is store
        assert len(left._cursor_key) == len(right._cursor_key) == 32
        assert left._cursor_key != right._cursor_key
        token = encode_result_cursor(
            left._cursor_key, IndexCursor(1_704_067_200_000, "foreign-result")
        )
        with pytest.raises(InvalidBacktestResultHttpRequest):
            right.list_index(parse_result_query("index", f"cursor={token}".encode()))
        assert store.get_calls == 0
        factory.assert_not_called()
    finally:
        assert first.shutdown(1)
        assert second.shutdown(1)
