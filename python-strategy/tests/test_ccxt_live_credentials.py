import ast
import importlib
import inspect

import pytest


def _owner():
    return importlib.import_module("src.core.adapters.ccxt_live_credentials")


def _valid_environ() -> dict[str, str]:
    return {
        "EXCHANGE_API_KEY": " key ",
        "EXCHANGE_SECRET": " secret ",
        "EXCHANGE_TESTNET": " true ",
    }


def test_build_ccxt_live_credentials_returns_exact_shared_values() -> None:
    assert _owner().build_ccxt_live_credentials(_valid_environ()) == {
        "api_key": "key",
        "secret": "secret",
        "testnet": True,
    }


@pytest.mark.parametrize("name", ["EXCHANGE_API_KEY", "EXCHANGE_SECRET"])
@pytest.mark.parametrize("raw", [None, "", "   "])
def test_build_ccxt_live_credentials_requires_nonblank_secrets(
    name: str,
    raw: str | None,
) -> None:
    environ = _valid_environ()
    if raw is None:
        environ.pop(name)
    else:
        environ[name] = raw

    with pytest.raises(ValueError) as raised:
        _owner().build_ccxt_live_credentials(environ)

    assert raised.value.args == (f"{name} must be set explicitly",)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", True),
        ("TRUE", True),
        (" yes ", True),
        ("On", True),
        ("0", False),
        ("FALSE", False),
        (" no ", False),
        ("Off", False),
    ],
)
def test_build_ccxt_live_credentials_parses_all_boolean_aliases(
    raw: str,
    expected: bool,
) -> None:
    environ = _valid_environ()
    environ["EXCHANGE_TESTNET"] = raw

    assert _owner().build_ccxt_live_credentials(environ)["testnet"] is expected


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_build_ccxt_live_credentials_requires_testnet_before_parsing(
    raw: str | None,
) -> None:
    environ = _valid_environ()
    if raw is None:
        environ.pop("EXCHANGE_TESTNET")
    else:
        environ["EXCHANGE_TESTNET"] = raw

    with pytest.raises(ValueError) as raised:
        _owner().build_ccxt_live_credentials(environ)

    assert raised.value.args == ("EXCHANGE_TESTNET must be set explicitly",)


def test_build_ccxt_live_credentials_rejects_ambiguous_testnet() -> None:
    environ = _valid_environ()
    environ["EXCHANGE_TESTNET"] = "enabled"

    with pytest.raises(ValueError) as raised:
        _owner().build_ccxt_live_credentials(environ)

    assert raised.value.args == ("EXCHANGE_TESTNET must be a boolean",)


@pytest.mark.parametrize(
    ("updates", "expected"),
    [
        (
            {
                "EXCHANGE_API_KEY": "",
                "EXCHANGE_SECRET": "",
                "EXCHANGE_TESTNET": "enabled",
            },
            "EXCHANGE_API_KEY must be set explicitly",
        ),
        (
            {"EXCHANGE_SECRET": "", "EXCHANGE_TESTNET": "enabled"},
            "EXCHANGE_SECRET must be set explicitly",
        ),
        (
            {"EXCHANGE_TESTNET": ""},
            "EXCHANGE_TESTNET must be set explicitly",
        ),
        (
            {"EXCHANGE_TESTNET": "enabled"},
            "EXCHANGE_TESTNET must be a boolean",
        ),
    ],
)
def test_build_ccxt_live_credentials_has_deterministic_error_precedence(
    updates: dict[str, str],
    expected: str,
) -> None:
    environ = _valid_environ()
    environ.update(updates)

    with pytest.raises(ValueError) as raised:
        _owner().build_ccxt_live_credentials(environ)

    assert raised.value.args == (expected,)


def test_ccxt_credential_owner_has_no_provider_or_account_policy() -> None:
    owner = _owner()
    source = inspect.getsource(owner)

    assert tuple(inspect.signature(owner.build_ccxt_live_credentials).parameters) == (
        "environ",
    )
    for forbidden in (
        "EXCHANGE_ENABLE_WS",
        "ACCOUNT_",
        "RITHMIC_",
        "BINANCE_",
        "BACKPACK_",
        "BYBIT_",
    ):
        assert forbidden not in source


def test_ccxt_credential_owner_has_only_the_mapping_dependency() -> None:
    tree = ast.parse(inspect.getsource(_owner()))
    imports = [
        node for node in ast.walk(tree) if isinstance(node, ast.Import | ast.ImportFrom)
    ]

    assert len(imports) == 1
    assert isinstance(imports[0], ast.ImportFrom)
    assert imports[0].module == "collections.abc"
    assert [(alias.name, alias.asname) for alias in imports[0].names] == [
        ("Mapping", None)
    ]
