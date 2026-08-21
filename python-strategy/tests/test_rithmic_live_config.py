from pathlib import Path

import pytest

from src.core.adapters.rithmic_live_config import (
    build_rithmic_live_adapter_config,
    validate_rithmic_recovery_identity,
)


def _environment(credentials_path: Path) -> dict[str, str]:
    return {
        "RITHMIC_PROFILE": " orders ",
        "RITHMIC_ACCOUNT_ID": " ACCOUNT ",
        "RITHMIC_INSTRUMENTS_JSON": (
            '{"RITHMIC:NQ-202609":{"exchange":"CME","quantity_step":"1",'
            '"price_tick":"0.25"}}'
        ),
        "FLUXTRADE_CREDENTIALS_PATH": str(credentials_path),
    }


def test_build_rithmic_live_config_owns_exact_existing_fields() -> None:
    environment = _environment(Path(__file__))
    environment["RITHMIC_RECOVERY_PROFILE"] = " recovery "

    assert build_rithmic_live_adapter_config(
        product_ids=["RITHMIC:NQ-202609"],
        environ=environment,
    ) == {
        "rithmic_profile": "orders",
        "account_id": "ACCOUNT",
        "rithmic_instruments": {
            "RITHMIC:NQ-202609": {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
        "rithmic_recovery_profile": "recovery",
        "rithmic_recovery_account_id": "ACCOUNT",
    }


def test_same_profile_preserves_each_explicit_account_identity() -> None:
    first = _environment(Path(__file__))
    second = dict(first)
    first["RITHMIC_ACCOUNT_ID"] = "ACCOUNT-A"
    second["RITHMIC_ACCOUNT_ID"] = "ACCOUNT-B"

    first_config = build_rithmic_live_adapter_config(
        product_ids=["RITHMIC:NQ-202609"],
        environ=first,
    )
    second_config = build_rithmic_live_adapter_config(
        product_ids=["RITHMIC:NQ-202609"],
        environ=second,
    )

    assert first_config["rithmic_profile"] == "orders"
    assert second_config["rithmic_profile"] == "orders"
    assert first_config["account_id"] == "ACCOUNT-A"
    assert second_config["account_id"] == "ACCOUNT-B"
    assert first_config["rithmic_recovery_account_id"] == "ACCOUNT-A"
    assert second_config["rithmic_recovery_account_id"] == "ACCOUNT-B"


@pytest.mark.parametrize(
    ("recovery_value", "expected"),
    [
        (None, "orders"),
        ("", "orders"),
        (" recovery ", "recovery"),
        ("   ", ""),
    ],
)
def test_recovery_profile_preserves_existing_fallback_and_strip(
    recovery_value: str | None,
    expected: str,
) -> None:
    environment = _environment(Path(__file__))
    if recovery_value is not None:
        environment["RITHMIC_RECOVERY_PROFILE"] = recovery_value

    config = build_rithmic_live_adapter_config(
        product_ids=["RITHMIC:NQ-202609"],
        environ=environment,
    )

    assert config["rithmic_recovery_profile"] == expected


@pytest.mark.parametrize(
    "env_name",
    [
        "RITHMIC_PROFILE",
        "RITHMIC_ACCOUNT_ID",
        "RITHMIC_INSTRUMENTS_JSON",
        "FLUXTRADE_CREDENTIALS_PATH",
    ],
)
def test_required_rithmic_values_keep_exact_failure(
    env_name: str,
) -> None:
    environment = _environment(Path(__file__))
    del environment[env_name]

    with pytest.raises(ValueError, match=env_name):
        build_rithmic_live_adapter_config(
            product_ids=["RITHMIC:NQ-202609"],
            environ=environment,
        )


@pytest.mark.parametrize("raw_value", ["not-json", "[]", "{}", "null"])
def test_instrument_map_rejects_invalid_json_object(raw_value: str) -> None:
    environment = _environment(Path(__file__))
    environment["RITHMIC_INSTRUMENTS_JSON"] = raw_value

    with pytest.raises(ValueError, match="RITHMIC_INSTRUMENTS_JSON"):
        build_rithmic_live_adapter_config(
            product_ids=["RITHMIC:NQ-202609"],
            environ=environment,
        )


@pytest.mark.parametrize(
    ("product_ids", "raw_instruments"),
    [
        (
            ["RITHMIC:NQ-202609", "RITHMIC:MNQ-202609"],
            '{"RITHMIC:NQ-202609":{"exchange":"CME"}}',
        ),
        (
            ["RITHMIC:NQ-202609"],
            (
                '{"RITHMIC:NQ-202609":{"exchange":"CME"},'
                '"RITHMIC:MNQ-202609":{"exchange":"CME"}}'
            ),
        ),
    ],
)
def test_instrument_map_requires_exact_selected_product_keys(
    product_ids: list[str],
    raw_instruments: str,
) -> None:
    environment = _environment(Path(__file__))
    environment["RITHMIC_INSTRUMENTS_JSON"] = raw_instruments

    with pytest.raises(ValueError, match="keys must match INSTRUMENT_PRODUCT_IDS"):
        build_rithmic_live_adapter_config(
            product_ids=product_ids,
            environ=environment,
        )


def test_credentials_path_must_identify_a_file(tmp_path: Path) -> None:
    environment = _environment(tmp_path / "missing.toml")

    with pytest.raises(
        ValueError,
        match="FLUXTRADE_CREDENTIALS_PATH must reference a file",
    ):
        build_rithmic_live_adapter_config(
            product_ids=["RITHMIC:NQ-202609"],
            environ=environment,
        )


@pytest.mark.parametrize(
    ("profile", "account_id", "expected_error"),
    [
        (None, None, None),
        ("orders", "ACCOUNT", None),
        ("orders", None, "rithmic_recovery_requires_account_id"),
        (None, "ACCOUNT", "rithmic_account_id_requires_recovery_profile"),
        ("", "ACCOUNT", "rithmic_account_id_requires_recovery_profile"),
    ],
)
def test_recovery_identity_validator_preserves_truthiness_matrix(
    profile: str | None,
    account_id: str | None,
    expected_error: str | None,
) -> None:
    config = {
        "rithmic_recovery_profile": profile,
        "rithmic_recovery_account_id": account_id,
    }

    if expected_error is None:
        validate_rithmic_recovery_identity(config)
        return

    with pytest.raises(ValueError, match=expected_error):
        validate_rithmic_recovery_identity(config)
