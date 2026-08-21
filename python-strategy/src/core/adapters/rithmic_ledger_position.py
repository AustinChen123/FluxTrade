from decimal import Decimal, InvalidOperation
from typing import Iterable, Mapping, Protocol

from src.core.interfaces.exchange import ExchangeError
from src.core.models import Position, PositionSide


class RithmicLedgerPosition(Protocol):
    exchange: object
    symbol: object
    net_quantity: object
    average_open_fill_price: object
    open_pnl: object


def project_rithmic_ledger_positions(
    snapshot: object,
    *,
    account_id: str,
    products_by_native_identity: Mapping[tuple[str, str], str],
) -> list[Position]:
    """Project one authoritative Rithmic account snapshot into live positions."""
    if str(getattr(snapshot, "account_id", "")).strip() != account_id:
        raise ExchangeError("rithmic_ledger_account_id_mismatch")

    remote_positions: Iterable[RithmicLedgerPosition] = getattr(snapshot, "positions")
    positions: list[Position] = []
    for remote in remote_positions:
        try:
            net_quantity = Decimal(str(remote.net_quantity))
        except (InvalidOperation, TypeError, ValueError) as error:
            raise ExchangeError(
                "rithmic_ledger_position_value_invalid: "
                f"exchange={remote.exchange} symbol={remote.symbol}"
            ) from error
        if not net_quantity.is_finite():
            raise ExchangeError(
                "rithmic_ledger_position_value_invalid: "
                f"exchange={remote.exchange} symbol={remote.symbol}"
            )
        if net_quantity == 0:
            continue
        identity = (
            str(remote.exchange).strip().upper(),
            str(remote.symbol).strip().upper(),
        )
        product_id = products_by_native_identity.get(identity)
        if product_id is None:
            raise ExchangeError(
                "rithmic_ledger_position_instrument_unmapped: "
                f"exchange={identity[0]} symbol={identity[1]}"
            )
        try:
            entry_price = Decimal(str(remote.average_open_fill_price or "0"))
            unrealized_pnl = Decimal(str(remote.open_pnl or "0"))
        except (InvalidOperation, TypeError, ValueError) as error:
            raise ExchangeError(
                "rithmic_ledger_position_value_invalid: "
                f"exchange={identity[0]} symbol={identity[1]}"
            ) from error
        if not all(value.is_finite() for value in (entry_price, unrealized_pnl)):
            raise ExchangeError(
                "rithmic_ledger_position_value_invalid: "
                f"exchange={identity[0]} symbol={identity[1]}"
            )
        positions.append(
            Position(
                strategy_id="LIVE",
                product_id=product_id,
                side=PositionSide.LONG if net_quantity > 0 else PositionSide.SHORT,
                quantity=abs(net_quantity),
                entry_price=entry_price,
                unrealized_pnl=unrealized_pnl,
            )
        )
    return positions
