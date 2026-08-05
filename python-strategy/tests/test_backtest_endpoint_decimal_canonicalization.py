from decimal import Context, Decimal

from src.control_plane.backtest_jobs import _json_safe
from src.core.backtest.endpoint_state import ReplayEndpointState


class _ForgedFormatDecimal(Decimal):
    def __format__(self, specifier: str, context: Context | None = None, /) -> str:
        return "999.00"

    def is_finite(self) -> bool:
        return False

    def is_zero(self) -> bool:
        return True


def test_endpoint_external_json_preserves_legacy_decimal_canonicalization() -> None:
    plain = ReplayEndpointState(final_mark=Decimal("1000"), end_timestamp=1)
    exponent = ReplayEndpointState(final_mark=Decimal("1E+3"), end_timestamp=1)

    plain_json = _json_safe(plain)
    exponent_json = _json_safe(exponent)

    assert plain_json == exponent_json
    assert plain_json["final_mark"] == "1000"
    assert exponent_json["final_mark"] == "1000"


def test_endpoint_projects_decimal_subclass_to_base_value() -> None:
    forged = _ForgedFormatDecimal("1.25")
    endpoint = ReplayEndpointState(final_mark=forged, end_timestamp=1)

    assert endpoint.final_mark == Decimal("1.25")
    assert endpoint.final_mark != Decimal("999.00")
    assert type(endpoint.final_mark) is Decimal
