"""Rithmic-owned live environment configuration policy."""

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


def _required_value(environ: Mapping[str, str], name: str) -> str:
    value = (environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} must be set explicitly")
    return value


def _instrument_map(environ: Mapping[str, str]) -> dict[str, Any]:
    raw = _required_value(environ, "RITHMIC_INSTRUMENTS_JSON")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("RITHMIC_INSTRUMENTS_JSON must be valid JSON") from exc
    if not isinstance(value, dict) or not value:
        raise ValueError("RITHMIC_INSTRUMENTS_JSON must be a non-empty JSON object")
    return value


def build_rithmic_live_adapter_config(
    *,
    product_ids: Sequence[str],
    environ: Mapping[str, str],
) -> dict[str, Any]:
    """Build the provider-specific portion of one Rithmic adapter config."""
    profile = _required_value(environ, "RITHMIC_PROFILE")
    account_id = _required_value(environ, "RITHMIC_ACCOUNT_ID")
    instruments = _instrument_map(environ)
    if set(instruments) != set(product_ids):
        raise ValueError(
            "RITHMIC_INSTRUMENTS_JSON keys must match INSTRUMENT_PRODUCT_IDS"
        )
    credentials_path = Path(_required_value(environ, "FLUXTRADE_CREDENTIALS_PATH"))
    if not credentials_path.is_file():
        raise ValueError("FLUXTRADE_CREDENTIALS_PATH must reference a file")
    recovery_profile = (environ.get("RITHMIC_RECOVERY_PROFILE") or profile).strip()
    return {
        "rithmic_profile": profile,
        "account_id": account_id,
        "rithmic_instruments": instruments,
        "rithmic_recovery_profile": recovery_profile,
        "rithmic_recovery_account_id": account_id,
    }


def validate_rithmic_recovery_identity(config: Mapping[str, object]) -> None:
    """Preserve the existing paired truthiness contract for recovery identity."""
    if config.get("rithmic_recovery_profile") and not config.get(
        "rithmic_recovery_account_id"
    ):
        raise ValueError("rithmic_recovery_requires_account_id")
    if config.get("rithmic_recovery_account_id") and not config.get(
        "rithmic_recovery_profile"
    ):
        raise ValueError("rithmic_account_id_requires_recovery_profile")
