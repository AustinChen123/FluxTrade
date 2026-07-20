from unittest.mock import patch

import pytest

from src.core.runtime_environment import RuntimeEnvironment, validate_environment_identity


@pytest.mark.parametrize("identity", ["live", "test", "paper-2"])
def test_environment_identity_builds_isolated_keys(identity):
    environment = RuntimeEnvironment(validate_environment_identity(identity))

    assert environment.key("system:state") == f"fluxtrade:{identity}:system:state"
    assert environment.key("heartbeat:python") == f"fluxtrade:{identity}:heartbeat:python"


@pytest.mark.parametrize("identity", ["", " test", "TEST", "test_1", "-test", "test-"])
def test_environment_identity_rejects_ambiguous_values(identity):
    with pytest.raises(ValueError):
        validate_environment_identity(identity)


def test_missing_environment_identity_fails_closed():
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ValueError, match="must be set explicitly"):
            RuntimeEnvironment.from_env()


def test_only_live_environment_can_execute_external_kill():
    with patch.dict("os.environ", {"FLUXTRADE_ENVIRONMENT": "live"}, clear=True):
        assert RuntimeEnvironment.from_env().allows_external_kill
    with patch.dict("os.environ", {"FLUXTRADE_ENVIRONMENT": "test"}, clear=True):
        assert not RuntimeEnvironment.from_env().allows_external_kill
