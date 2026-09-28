"""Fresh-artifact smoke for the opaque native surface; no production consumer."""
import json

import pytest

pytestmark = pytest.mark.rust


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
