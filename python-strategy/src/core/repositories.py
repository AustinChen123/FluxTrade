import time
from collections.abc import Mapping
from contextlib import contextmanager
from decimal import Decimal
from threading import Lock
from typing import Callable, ContextManager, Iterator, Optional, cast

from sqlalchemy import func
from sqlalchemy.orm import Query, Session

from src.core.interfaces import IOrderRepository
from src.core.interfaces.conditional_orders import ConditionalOrderRecord
from src.core.interfaces.order_cancellation import OrderCancellationSnapshot
from src.core.interfaces.verified_net_reduction import (
    VerifiedNetReductionOrderSnapshot,
)
from src.core.models import OrderSide, OrderStatus
from src.core.orm_models import BacktestTradeLog, Order, Position, Trade
from src.core.product_master import ensure_product_registered


@contextmanager
def _provided_session(session: Session | None) -> Iterator[Session]:
    if session is None:
        raise RuntimeError("database session is required")
    yield session


class LiveOrderRepository(IOrderRepository):
    def __init__(
        self,
        db_session: Session | None = None,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
        account_profile: str | None = None,
        account_id: str | None = None,
    ):
        if (account_profile is None) != (account_id is None):
            raise ValueError("repository account identity must be complete")
        if account_profile is not None and account_id is not None:
            account_profile = account_profile.strip()
            account_id = account_id.strip()
            if not account_profile or not account_id:
                raise ValueError("repository account identity must not be blank")
        self._account_profile = account_profile
        self._account_id = account_id
        self._db_session_factory = db_session_factory or (
            lambda: _provided_session(db_session)
        )

    def _scope(self, query: Query) -> Query:
        if self._account_profile is None:
            return query
        return query.filter(
            Order.account_profile == self._account_profile,
            Order.account_id == self._account_id,
        )

    def _bind_order(self, order: Order) -> None:
        if self._account_profile is None:
            return
        current = (order.account_profile, order.account_id)
        if current == (None, None):
            order.account_profile = self._account_profile
            order.account_id = self._account_id
        elif current != (self._account_profile, self._account_id):
            raise RuntimeError("order_account_identity_mismatch")

    @staticmethod
    def _reject_legacy_collision(db: Session, order: Order) -> None:
        query = db.query(Order).filter(
            Order.account_profile.is_(None),
            Order.account_id.is_(None),
        )
        client_collision = (
            order.client_order_id is not None
            and query.filter(Order.client_order_id == order.client_order_id).first()
            is not None
        )
        exchange_collision = (
            order.exchange_order_id is not None
            and query.filter(
                Order.exchange_id == order.exchange_id,
                Order.exchange_order_id == order.exchange_order_id,
            ).first()
            is not None
        )
        if client_collision or exchange_collision:
            raise RuntimeError("order_account_identity_legacy_collision")

    def add_order(self, order: Order) -> None:
        self._bind_order(order)
        with self._db_session_factory() as db:
            if self._account_profile is not None:
                self._reject_legacy_collision(db, order)
            ensure_product_registered(db, str(order.product_id))
            db.add(order)
            db.commit()
            db.refresh(order)

    def update_order(self, order: Order) -> None:
        self._bind_order(order)
        with self._db_session_factory() as db:
            db.add(order)
            db.commit()
            db.refresh(order)

    def persist_conditional_order(self, order: ConditionalOrderRecord) -> None:
        self.update_order(cast(Order, order))

    def get_order(self, order_id: str) -> Optional[Order]:
        with self._db_session_factory() as db:
            return self._scope(db.query(Order)).filter_by(id=order_id).first()

    @staticmethod
    def _verified_net_reduction_snapshot(
        order: Order,
    ) -> VerifiedNetReductionOrderSnapshot:
        payload = order.intent_payload
        return VerifiedNetReductionOrderSnapshot(
            id=str(order.id),
            client_order_id=order.client_order_id,
            strategy_id=order.strategy_id,
            product_id=str(order.product_id),
            type=str(order.type),
            side=str(getattr(order.side, "value", order.side)),
            quantity=order.quantity,
            filled_quantity=order.filled_quantity,
            status=str(order.status),
            intent_payload=payload if isinstance(payload, Mapping) else None,
        )

    def get_verified_net_reduction_order(
        self,
        order_id: str,
    ) -> VerifiedNetReductionOrderSnapshot | None:
        order = self.get_order(order_id)
        return (
            self._verified_net_reduction_snapshot(order) if order is not None else None
        )

    def get_verified_net_reduction_order_by_client_id(
        self,
        client_order_id: str,
    ) -> VerifiedNetReductionOrderSnapshot | None:
        order = self.get_order_by_client_order_id(client_order_id)
        return (
            self._verified_net_reduction_snapshot(order) if order is not None else None
        )

    def persist_verified_net_reduction(
        self,
        order_id: str,
        intent_payload: Mapping[str, object],
    ) -> None:
        with self._db_session_factory() as db:
            order = self._scope(db.query(Order)).filter_by(id=order_id).first()
            if order is None:
                raise RuntimeError("verified_net_reduction_order_not_found")
            order.intent_payload = dict(intent_payload)
            db.commit()

    def get_conditional_order(
        self,
        order_id: str,
    ) -> ConditionalOrderRecord | None:
        return self.get_order(order_id)

    def get_order_for_cancellation(
        self,
        order_id: str,
    ) -> OrderCancellationSnapshot | None:
        order = self.get_order(order_id)
        if order is None:
            return None
        return OrderCancellationSnapshot(
            id=str(order.id),
            product_id=str(order.product_id),
            type=str(order.type),
            status=str(order.status),
            filled_quantity=order.filled_quantity,
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
        )

    def mark_order_cancelled(self, order_id: str) -> None:
        with self._db_session_factory() as db:
            order = self._scope(db.query(Order)).filter_by(id=order_id).first()
            if order is None:
                raise RuntimeError("cancellation_order_not_found")
            order.status = OrderStatus.CANCELLED.value
            db.commit()

    def get_order_by_client_order_id(self, client_order_id: str) -> Optional[Order]:
        with self._db_session_factory() as db:
            return (
                self._scope(db.query(Order))
                .filter_by(client_order_id=client_order_id)
                .first()
            )

    def get_order_by_exchange_order_id(
        self,
        exchange_order_id: str,
        exchange_id: str | None = None,
        product_id: str | None = None,
    ) -> Optional[Order]:
        with self._db_session_factory() as db:
            query = self._scope(db.query(Order)).filter_by(
                exchange_order_id=exchange_order_id
            )
            if exchange_id is not None:
                query = query.filter_by(exchange_id=exchange_id)
            if product_id is not None:
                query = query.filter_by(product_id=product_id)
            return query.first()

    def list_client_orders_by_statuses(
        self,
        statuses: set[str],
        exchange_id: str | None = None,
    ) -> list[Order]:
        if not statuses:
            return []
        with self._db_session_factory() as db:
            query = db.query(Order).filter(
                Order.status.in_(statuses),
                Order.client_order_id.isnot(None),
            )
            query = self._scope(query)
            if exchange_id is not None:
                query = query.filter(
                    func.lower(Order.exchange_id) == exchange_id.casefold()
                )
            return query.all()

    def list_orders_by_statuses(
        self,
        statuses: set[str],
        exchange_id: str | None = None,
    ) -> list[Order]:
        if not statuses:
            return []
        with self._db_session_factory() as db:
            query = db.query(Order).filter(Order.status.in_(statuses))
            query = self._scope(query)
            if exchange_id is not None:
                query = query.filter(
                    func.lower(Order.exchange_id) == exchange_id.casefold()
                )
            return query.all()

    def list_conditional_orders_by_statuses(
        self,
        statuses: set[str],
        exchange_id: str | None = None,
    ) -> list[ConditionalOrderRecord]:
        return cast(
            list[ConditionalOrderRecord],
            self.list_orders_by_statuses(statuses, exchange_id),
        )

    def list_legacy_orders_by_statuses(
        self,
        statuses: set[str],
        exchange_id: str | None = None,
    ) -> list[Order]:
        if self._account_profile is None or not statuses:
            return []
        with self._db_session_factory() as db:
            query = db.query(Order).filter(
                Order.status.in_(statuses),
                Order.client_order_id.isnot(None),
                Order.account_profile.is_(None),
                Order.account_id.is_(None),
            )
            if exchange_id is not None:
                query = query.filter(
                    func.lower(Order.exchange_id) == exchange_id.casefold()
                )
            return query.all()

    def add_trade(self, trade: Trade) -> None:
        with self._db_session_factory() as db:
            ensure_product_registered(db, str(trade.product_id))
            db.add(trade)
            db.commit()

    def persist_fill(self, order: Order, trade: Trade) -> None:
        self._bind_order(order)
        with self._db_session_factory() as db:
            ensure_product_registered(db, str(trade.product_id))
            db.add(order)
            db.add(trade)
            db.commit()
            db.refresh(order)

    def get_position(
        self, strategy_id: str, product_id: str, side: str
    ) -> Optional[Position]:
        with self._db_session_factory() as db:
            return (
                db.query(Position)
                .filter_by(strategy_id=strategy_id, product_id=product_id, side=side)
                .first()
            )

    def update_position(
        self,
        strategy_id: str,
        product_id: str,
        side: str,
        fill_quantity: Decimal,
        fill_price: Decimal,
        position_side: str,
    ) -> None:
        # Use with_for_update for locking
        with self._db_session_factory() as db:
            ensure_product_registered(db, product_id)
            position = (
                db.query(Position)
                .with_for_update()
                .filter_by(
                    strategy_id=strategy_id, product_id=product_id, side=position_side
                )
                .first()
            )

            current_time = int(time.time() * 1000)

            if not position:
                if side == OrderSide.BUY:
                    position = Position(
                        strategy_id=strategy_id,
                        product_id=product_id,
                        side=position_side,
                        quantity=Decimal("0"),
                        entry_price=Decimal("0"),
                        unrealized_pnl=Decimal("0"),
                        last_update_timestamp=current_time,
                    )
                    db.add(position)
                else:
                    db.commit()
                    return

            if side == OrderSide.BUY:
                total_cost = (position.quantity * position.entry_price) + (
                    fill_quantity * fill_price
                )
                total_qty = position.quantity + fill_quantity
                if total_qty > 0:
                    position.entry_price = total_cost / total_qty
                position.quantity = total_qty
            elif side == OrderSide.SELL:
                position.quantity = max(Decimal("0"), position.quantity - fill_quantity)

            position.last_update_timestamp = current_time
            db.commit()

    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        self._bind_order(order)
        order.exchange_order_id = exchange_order_id
        with self._db_session_factory() as db:
            db.add(order)
            db.commit()
            db.refresh(order)


class BacktestOrderRepository(IOrderRepository):
    """Order repository for backtest mode.

    Balance and position tracking are delegated to the Rust
    PyMatchingEngine via BacktestAccountService + SimulatedAdapter.
    This repository only records trade logs to the database.
    """

    def __init__(
        self,
        db_session: Session | None,
        session_id: int,
        initial_balance: Decimal = Decimal("10000"),
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
    ):
        self._db_session_factory = db_session_factory or (
            lambda: _provided_session(db_session)
        )
        self.session_id = session_id
        self.balance = initial_balance  # kept for backward compatibility
        self._order_strategy_map: dict[str, str] = {}
        self._next_fill_sequence = 0
        self._trade_write_lock = Lock()

    def add_order(self, order: Order) -> None:
        # Track order → strategy_id for BacktestTradeLog
        if order.strategy_id:
            self._order_strategy_map[order.id] = order.strategy_id

    def update_order(self, order: Order) -> None:
        pass

    def persist_conditional_order(self, order: ConditionalOrderRecord) -> None:
        self.update_order(cast(Order, order))

    def get_order(self, order_id: str) -> Optional[Order]:
        return None

    def get_verified_net_reduction_order(
        self,
        order_id: str,
    ) -> VerifiedNetReductionOrderSnapshot | None:
        return None

    def get_verified_net_reduction_order_by_client_id(
        self,
        client_order_id: str,
    ) -> VerifiedNetReductionOrderSnapshot | None:
        return None

    def persist_verified_net_reduction(
        self,
        order_id: str,
        intent_payload: Mapping[str, object],
    ) -> None:
        raise RuntimeError("verified_net_reduction_order_not_found")

    def get_conditional_order(
        self,
        order_id: str,
    ) -> ConditionalOrderRecord | None:
        return self.get_order(order_id)

    def get_order_for_cancellation(
        self,
        order_id: str,
    ) -> OrderCancellationSnapshot | None:
        return None

    def mark_order_cancelled(self, order_id: str) -> None:
        raise RuntimeError("cancellation_order_not_found")

    def get_order_by_client_order_id(self, client_order_id: str) -> Optional[Order]:
        return None

    def get_order_by_exchange_order_id(
        self,
        exchange_order_id: str,
        exchange_id: str | None = None,
        product_id: str | None = None,
    ) -> Optional[Order]:
        return None

    def list_client_orders_by_statuses(
        self,
        statuses: set[str],
        exchange_id: str | None = None,
    ) -> list[Order]:
        return []

    def list_orders_by_statuses(
        self,
        statuses: set[str],
        exchange_id: str | None = None,
    ) -> list[Order]:
        return []

    def list_conditional_orders_by_statuses(
        self,
        statuses: set[str],
        exchange_id: str | None = None,
    ) -> list[ConditionalOrderRecord]:
        return cast(
            list[ConditionalOrderRecord],
            self.list_orders_by_statuses(statuses, exchange_id),
        )

    def add_trade(self, trade: Trade) -> None:
        with self._trade_write_lock:
            strategy_id = self._order_strategy_map.get(trade.order_id)
            bt_log = BacktestTradeLog(
                id=trade.id,
                session_id=self.session_id,
                strategy_id=strategy_id,
                order_id=trade.order_id,
                exchange_trade_id=trade.exchange_trade_id,
                product_id=trade.product_id,
                side=trade.side,
                price=trade.price,
                quantity=trade.quantity,
                fee=trade.fee,
                fee_asset=trade.fee_asset,
                timestamp=trade.timestamp,
                fill_sequence=self._next_fill_sequence,
            )
            with self._db_session_factory() as db:
                db.add(bt_log)
                db.commit()
            self._next_fill_sequence += 1

    def update_position(
        self,
        strategy_id: str,
        product_id: str,
        side: str,
        fill_quantity: Decimal,
        fill_price: Decimal,
        position_side: str,
    ) -> None:
        # No-op: position and balance are tracked by Rust PyMatchingEngine
        pass

    def get_position(
        self,
        strategy_id: str,
        product_id: str,
        side: str | None = None,
    ) -> Optional[Position]:
        # Position state lives in Rust engine; accessed via BacktestAccountService
        return None

    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        order.exchange_order_id = exchange_order_id
