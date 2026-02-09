import logging
import threading
from decimal import Decimal
from typing import Dict

logger = logging.getLogger(__name__)


class CapitalAllocator:
    """Manages per-strategy capital allocation from a shared balance pool.

    Thread-safe: all public methods acquire a lock before mutating state.
    All monetary values are Decimal — float is forbidden.
    """

    def __init__(self, total_balance: Decimal) -> None:
        if not isinstance(total_balance, Decimal):
            raise TypeError("total_balance must be Decimal")
        if total_balance < Decimal("0"):
            raise ValueError("total_balance must be non-negative")

        self._total_balance = total_balance
        self._allocations: Dict[str, Decimal] = {}
        self._used: Dict[str, Decimal] = {}
        self._lock = threading.Lock()

    # ── Allocation lifecycle ───────────────────────────────────────

    def allocate(self, strategy_id: str, amount: Decimal) -> None:
        """Allocate capital to a strategy. Raises if insufficient unallocated balance."""
        if not isinstance(amount, Decimal):
            raise TypeError("amount must be Decimal")
        if amount < Decimal("0"):
            raise ValueError("amount must be non-negative")

        with self._lock:
            unallocated = self._unallocated_unlocked()
            if amount > unallocated:
                raise ValueError(
                    f"Cannot allocate {amount}: only {unallocated} unallocated"
                )
            current = self._allocations.get(strategy_id, Decimal("0"))
            self._allocations[strategy_id] = current + amount
            logger.info(
                "Allocated %s to strategy %s (total: %s)",
                amount, strategy_id, self._allocations[strategy_id],
            )

    def deallocate(self, strategy_id: str) -> Decimal:
        """Return a strategy's allocation back to the pool.

        Returns the amount deallocated. Raises ValueError if the strategy
        still has capital in use.
        """
        with self._lock:
            allocation = self._allocations.get(strategy_id, Decimal("0"))
            used = self._used.get(strategy_id, Decimal("0"))
            if used > Decimal("0"):
                raise ValueError(
                    f"Cannot deallocate strategy {strategy_id}: "
                    f"{used} capital still in use"
                )
            self._allocations.pop(strategy_id, None)
            self._used.pop(strategy_id, None)
            logger.info("Deallocated %s from strategy %s", allocation, strategy_id)
            return allocation

    # ── Query methods ──────────────────────────────────────────────

    def get_available(self, strategy_id: str) -> Decimal:
        """Get available (allocated - used) capital for a strategy."""
        with self._lock:
            allocated = self._allocations.get(strategy_id, Decimal("0"))
            used = self._used.get(strategy_id, Decimal("0"))
            return allocated - used

    def get_allocation(self, strategy_id: str) -> Decimal:
        """Get total allocated capital for a strategy."""
        with self._lock:
            return self._allocations.get(strategy_id, Decimal("0"))

    def get_unallocated(self) -> Decimal:
        """Get remaining unallocated balance."""
        with self._lock:
            return self._unallocated_unlocked()

    # ── Usage tracking ─────────────────────────────────────────────

    def record_usage(self, strategy_id: str, amount: Decimal) -> None:
        """Record capital used (e.g., when opening a position)."""
        if not isinstance(amount, Decimal):
            raise TypeError("amount must be Decimal")
        if amount < Decimal("0"):
            raise ValueError("amount must be non-negative")

        with self._lock:
            allocated = self._allocations.get(strategy_id, Decimal("0"))
            current_used = self._used.get(strategy_id, Decimal("0"))
            if current_used + amount > allocated:
                raise ValueError(
                    f"Cannot use {amount} for strategy {strategy_id}: "
                    f"only {allocated - current_used} available"
                )
            self._used[strategy_id] = current_used + amount

    def release_usage(self, strategy_id: str, amount: Decimal) -> None:
        """Release used capital (e.g., when closing a position)."""
        if not isinstance(amount, Decimal):
            raise TypeError("amount must be Decimal")
        if amount < Decimal("0"):
            raise ValueError("amount must be non-negative")

        with self._lock:
            current_used = self._used.get(strategy_id, Decimal("0"))
            new_used = current_used - amount
            if new_used < Decimal("0"):
                raise ValueError(
                    f"Cannot release {amount} for strategy {strategy_id}: "
                    f"only {current_used} in use"
                )
            self._used[strategy_id] = new_used

    # ── Balance management ─────────────────────────────────────────

    @property
    def total_balance(self) -> Decimal:
        with self._lock:
            return self._total_balance

    def update_total_balance(self, new_balance: Decimal) -> None:
        """Update total balance (e.g., after PnL changes).

        Raises ValueError if new balance is less than the sum of
        all allocations (would leave strategies under-funded).
        """
        if not isinstance(new_balance, Decimal):
            raise TypeError("new_balance must be Decimal")
        if new_balance < Decimal("0"):
            raise ValueError("new_balance must be non-negative")

        with self._lock:
            total_allocated = sum(self._allocations.values(), Decimal("0"))
            if new_balance < total_allocated:
                raise ValueError(
                    f"Cannot set balance to {new_balance}: "
                    f"{total_allocated} already allocated"
                )
            self._total_balance = new_balance

    # ── Internal helpers ───────────────────────────────────────────

    def _unallocated_unlocked(self) -> Decimal:
        """Unallocated balance (caller must hold lock)."""
        total_allocated = sum(self._allocations.values(), Decimal("0"))
        return self._total_balance - total_allocated
