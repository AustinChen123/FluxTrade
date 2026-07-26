"""Idempotent persistence for canonical exchange and product identities."""

from __future__ import annotations

from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from src.core.orm_models import Exchange, Product
from src.core.product_registry import (
    to_base_quote,
    validate_product_id,
)


def ensure_product_registered(session: Session, product_id: str) -> None:
    """Ensure one canonical product and its venue exist in the current transaction."""
    validate_product_id(product_id)
    exchange_id = product_id.partition(":")[0]
    base_asset, quote_asset = to_base_quote(product_id)
    exchange_values = {
        "id": exchange_id,
        "name": exchange_id.title(),
    }
    product_values = {
        "id": product_id,
        "exchange_id": exchange_id,
        "base_asset": base_asset,
        "quote_asset": quote_asset,
    }

    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        session.execute(
            postgresql_insert(Exchange)
            .values(**exchange_values)
            .on_conflict_do_nothing(index_elements=["id"])
        )
        session.execute(
            postgresql_insert(Product)
            .values(**product_values)
            .on_conflict_do_nothing(index_elements=["id"])
        )
    elif dialect == "sqlite":
        session.execute(
            sqlite_insert(Exchange)
            .values(**exchange_values)
            .on_conflict_do_nothing(index_elements=["id"])
        )
        session.execute(
            sqlite_insert(Product)
            .values(**product_values)
            .on_conflict_do_nothing(index_elements=["id"])
        )
    else:
        if session.get(Exchange, exchange_id) is None:
            session.execute(insert(Exchange).values(**exchange_values))
        if session.get(Product, product_id) is None:
            session.execute(insert(Product).values(**product_values))

    product = session.get(Product, product_id)
    if product is None:
        raise RuntimeError(f"failed to register product: {product_id}")
    actual = (product.exchange_id, product.base_asset, product.quote_asset)
    expected = (exchange_id, base_asset, quote_asset)
    if actual != expected:
        raise ValueError(
            f"product master data conflicts with canonical identity: {product_id}"
        )
