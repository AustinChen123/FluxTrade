"""Fresh-artifact smoke for the opaque native surface; no production consumer."""
import json

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
