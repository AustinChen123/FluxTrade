"""Pure canonical checks for the private financial receipt projection."""

from decimal import Decimal

from src.core.spider_management.financial_delivery import (
    _projection,
    _same_projection,
)


def test_projection_uses_canonical_decimal_text_and_preserves_nullable_fields():
    first = _projection({
        "status": "PARTIALLY_FILLED",
        "filled_quantity": Decimal("1.2500"),
        "filled_price": Decimal("10.000"),
        "exchange_order_id": None,
    })
    second = _projection({
        "status": "PARTIALLY_FILLED",
        "filled_quantity": Decimal("1.25"),
        "filled_price": Decimal("10"),
        "exchange_order_id": None,
    })

    assert first == {
        "status": "PARTIALLY_FILLED",
        "filled_quantity": "1.25",
        "filled_price": "10",
        "exchange_order_id": None,
    }
    assert _same_projection(first, second)


def test_projection_distinguishes_provider_id_and_nullable_prices():
    before = _projection({
        "status": "NEW",
        "filled_quantity": Decimal("0"),
        "filled_price": Decimal("0"),
        "exchange_order_id": None,
    })
    without_provider_id = _projection({
        "status": "SUBMITTED",
        "filled_quantity": Decimal("0"),
        "filled_price": Decimal("0"),
        "exchange_order_id": None,
    })
    with_provider_id = {**without_provider_id, "exchange_order_id": "exchange-1"}
    nullable_price = {**without_provider_id, "filled_price": None}

    assert not _same_projection(before, without_provider_id)
    assert not _same_projection(without_provider_id, with_provider_id)
    assert not _same_projection(without_provider_id, nullable_price)


def test_projection_requires_the_exact_closed_four_field_shape():
    projection = {
        "status": "NEW",
        "filled_quantity": "0",
        "filled_price": "0",
        "exchange_order_id": None,
    }
    assert _same_projection(projection, dict(projection))
    assert not _same_projection(
        {key: value for key, value in projection.items() if key != "exchange_order_id"},
        projection,
    )
    assert not _same_projection(
        {**projection, "unexpected": None},
        projection,
    )


def test_private_main_composition_freezes_sources_and_preserves_scope_order():
    from dataclasses import replace

    from src.core.spider_management.runtime_owner import (
        ModelInstallSource,
        RuntimeScopeKey,
        SpiderRuntimeComposition,
    )
    from test_spider_model_bootstrap import _admitted_run, _confirmation, _product_specs

    run = replace(_admitted_run(), account_key="account-main")
    confirmation = _confirmation(run).request
    first = RuntimeScopeKey("live", "account-main", "spider-a")
    second = RuntimeScopeKey("live", "account-main", "spider-b")
    sources = {
        confirmation.request_id: ModelInstallSource(
            run, _product_specs(run), "okx"
        )
    }
    composition = SpiderRuntimeComposition((first, second), sources)

    assert composition.owned_scopes == (first, second)
    assert composition.model_sources_by_confirmation[confirmation.request_id].run is run
    sources["another-confirmation"] = ModelInstallSource(
        run, _product_specs(run), "okx"
    )
    assert "another-confirmation" not in composition.model_sources_by_confirmation
