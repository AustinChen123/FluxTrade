"""Fresh-artifact smoke for the opaque native surface; no production consumer."""
import json
from decimal import Decimal
from typing import cast

import pytest

pytestmark = pytest.mark.rust


def test_scheduler_classifier_uses_only_exact_closed_native_types():
    import fluxtrade_core as native
    from src.core.backtest.synthetic_scenario_codec import _native_failure

    for cls, kind, reason in [(native.ScenarioReplayInputError, "INPUT", "INVALID_SCHEMA"),
                             (native.ScenarioReplayLookupError, "LOOKUP", "UNKNOWN_RECEIPT_REFERENCE"),
                             (native.ScenarioReplayConflictError, "CONFLICT", "DELIVERY_ID_CONFLICT"),
                             (native.ScenarioReplayInvariantError, "INVARIANT", "NATIVE_INVARIANT")]:
        assert _native_failure(cls(reason)) == dict(kind=kind, reason=reason)
        assert _native_failure(type("Derived", (cls,), {})(reason)) is None
    assert all(_native_failure(error) is None for error in (ValueError("INVALID_SCHEMA"), LookupError("UNKNOWN_RECEIPT_REFERENCE"), RuntimeError("NATIVE_INVARIANT")))


def test_native_session_registered_and_closed_profiles_are_canonical():
    import fluxtrade_core as native

    key = '{"venue":"okx-scenario","environment":"test","account":"A"}'
    for profile in (
        "SYNTHETIC_BTC_ETH_V1",
        "SYNTHETIC_GOLDEN_CANCEL_V1",
        "SYNTHETIC_MIN_CASH_V1",
    ):
        session = native._SyntheticScenarioReplaySession(profile, key)
        raw = session.inspect_state()
        assert raw == session.inspect_state()
        assert raw == json.dumps(
            json.loads(raw), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        assert not hasattr(session, "owner")
    with pytest.raises(native.ScenarioReplayInputError) as error:
        native._SyntheticScenarioReplaySession("unknown", key)
    assert error.value.args == ("INVALID_SCHEMA",)
    assert str(error.value) == "INVALID_SCHEMA"


def test_codec_transports_p1_and_configured_constructor_arguments(monkeypatch):
    import fluxtrade_core as native
    from src.core.backtest import synthetic_scenario_codec as codec

    key = cast(codec.Account, {"venue": "okx-scenario", "environment": "test", "account": "A"})
    configuration = {
        "schema_version": "synthetic_multi_product_config_v1",
        "config_id": "native-boundary-test",
        "seed_effective_at": 50,
        "cash": Decimal("10"),
        "leverage": Decimal("1"),
        "products": [{
            "product_id": "WIRE-Z",
            "instrument_code": 1,
            "taker_fee_rate": Decimal("0.001"),
            "liquidation_fee_rate": Decimal("0.002"),
            "specs": [{"version": "s1", "valid_from": 0, "valid_to": None,
                "contract_value": Decimal("1"), "multiplier": Decimal("1"),
                "price_tick": Decimal("1"), "quantity_step": Decimal("0.5"),
                "minimum_quantity": Decimal("0.5")}],
            "tiers": [{"version": "t1", "valid_from": 0, "valid_to": None,
                "rows": [{"minimum_contracts": Decimal("0"), "maximum_contracts": Decimal("10"),
                    "mmr": Decimal("0.005"), "imr": Decimal("0.1"), "max_leverage": Decimal("10")}]}],
            "marks": [{"valid_from": 0, "valid_to": 100, "mark": Decimal("1")}],
        }],
        "positions": [],
        "orders": [],
    }
    calls = []

    class Session:
        def __init__(self, *args):
            calls.append(args)

    monkeypatch.setattr(native, "_SyntheticScenarioReplaySession", Session)
    codec.ScenarioCodec("SYNTHETIC_BTC_ETH_V1", key)
    codec.ScenarioCodec("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", key, configuration)
    assert calls == [
        ("SYNTHETIC_BTC_ETH_V1", codec._encode(key)),
        ("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", codec._encode(key), codec._encode_configuration(configuration)),
    ]
