"""
Tests for src/core/capital_allocator.py

Covers:
- Allocate/deallocate lifecycle
- Over-allocation rejection
- Usage recording and release
- Multiple strategies
- Thread safety (basic)
- Edge cases (zero allocation, negative amounts)
- Balance updates
"""

import threading
from decimal import Decimal

import pytest

from src.core.capital_allocator import CapitalAllocator


class TestCapitalAllocatorInit:
    """Tests for CapitalAllocator initialization."""

    def test_init_with_valid_balance(self):
        allocator = CapitalAllocator(Decimal("100000"))
        assert allocator.total_balance == Decimal("100000")
        assert allocator.get_unallocated() == Decimal("100000")

    def test_init_with_zero_balance(self):
        allocator = CapitalAllocator(Decimal("0"))
        assert allocator.total_balance == Decimal("0")

    def test_init_rejects_negative_balance(self):
        with pytest.raises(ValueError, match="non-negative"):
            CapitalAllocator(Decimal("-100"))

    def test_init_rejects_float(self):
        with pytest.raises(TypeError, match="Decimal"):
            CapitalAllocator(100000.0)  # type: ignore[arg-type]


class TestAllocateDeallocate:
    """Tests for allocation and deallocation lifecycle."""

    def test_allocate_basic(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        assert allocator.get_allocation("strat_a") == Decimal("30000")
        assert allocator.get_unallocated() == Decimal("70000")

    def test_allocate_multiple_strategies(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        allocator.allocate("strat_b", Decimal("50000"))

        assert allocator.get_allocation("strat_a") == Decimal("30000")
        assert allocator.get_allocation("strat_b") == Decimal("50000")
        assert allocator.get_unallocated() == Decimal("20000")

    def test_allocate_incremental(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("20000"))
        allocator.allocate("strat_a", Decimal("10000"))
        assert allocator.get_allocation("strat_a") == Decimal("30000")

    def test_allocate_exact_remaining(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("100000"))
        assert allocator.get_unallocated() == Decimal("0")

    def test_allocate_over_budget_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("80000"))
        with pytest.raises(ValueError, match="Cannot allocate"):
            allocator.allocate("strat_b", Decimal("30000"))

    def test_allocate_zero_is_allowed(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("0"))
        assert allocator.get_allocation("strat_a") == Decimal("0")

    def test_allocate_negative_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        with pytest.raises(ValueError, match="non-negative"):
            allocator.allocate("strat_a", Decimal("-1000"))

    def test_allocate_float_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        with pytest.raises(TypeError, match="Decimal"):
            allocator.allocate("strat_a", 30000.0)  # type: ignore[arg-type]

    def test_deallocate_returns_amount(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        returned = allocator.deallocate("strat_a")
        assert returned == Decimal("30000")
        assert allocator.get_unallocated() == Decimal("100000")

    def test_deallocate_nonexistent_returns_zero(self):
        allocator = CapitalAllocator(Decimal("100000"))
        returned = allocator.deallocate("no_such_strategy")
        assert returned == Decimal("0")

    def test_deallocate_with_usage_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        allocator.record_usage("strat_a", Decimal("10000"))
        with pytest.raises(ValueError, match="still in use"):
            allocator.deallocate("strat_a")

    def test_deallocate_after_release(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        allocator.record_usage("strat_a", Decimal("10000"))
        allocator.release_usage("strat_a", Decimal("10000"))
        returned = allocator.deallocate("strat_a")
        assert returned == Decimal("30000")


class TestUsageTracking:
    """Tests for recording and releasing capital usage."""

    def test_record_usage_basic(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        allocator.record_usage("strat_a", Decimal("10000"))
        assert allocator.get_available("strat_a") == Decimal("20000")

    def test_record_usage_full_allocation(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        allocator.record_usage("strat_a", Decimal("30000"))
        assert allocator.get_available("strat_a") == Decimal("0")

    def test_record_usage_exceeds_available_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        with pytest.raises(ValueError, match="Cannot use"):
            allocator.record_usage("strat_a", Decimal("30001"))

    def test_record_usage_unallocated_strategy_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        with pytest.raises(ValueError, match="Cannot use"):
            allocator.record_usage("strat_a", Decimal("1000"))

    def test_record_usage_negative_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        with pytest.raises(ValueError, match="non-negative"):
            allocator.record_usage("strat_a", Decimal("-100"))

    def test_record_usage_float_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        with pytest.raises(TypeError, match="Decimal"):
            allocator.record_usage("strat_a", 10000.0)  # type: ignore[arg-type]

    def test_release_usage_basic(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        allocator.record_usage("strat_a", Decimal("10000"))
        allocator.release_usage("strat_a", Decimal("5000"))
        assert allocator.get_available("strat_a") == Decimal("25000")

    def test_release_usage_full(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        allocator.record_usage("strat_a", Decimal("10000"))
        allocator.release_usage("strat_a", Decimal("10000"))
        assert allocator.get_available("strat_a") == Decimal("30000")

    def test_release_usage_exceeds_used_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        allocator.record_usage("strat_a", Decimal("10000"))
        with pytest.raises(ValueError, match="Cannot release"):
            allocator.release_usage("strat_a", Decimal("10001"))

    def test_release_usage_nothing_in_use_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        with pytest.raises(ValueError, match="Cannot release"):
            allocator.release_usage("strat_a", Decimal("1"))

    def test_release_usage_negative_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        with pytest.raises(ValueError, match="non-negative"):
            allocator.release_usage("strat_a", Decimal("-100"))

    def test_release_usage_float_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        with pytest.raises(TypeError, match="Decimal"):
            allocator.release_usage("strat_a", 5000.0)  # type: ignore[arg-type]


class TestQueryMethods:
    """Tests for get_available, get_allocation, get_unallocated."""

    def test_get_available_no_allocation(self):
        allocator = CapitalAllocator(Decimal("100000"))
        assert allocator.get_available("unknown") == Decimal("0")

    def test_get_allocation_no_allocation(self):
        allocator = CapitalAllocator(Decimal("100000"))
        assert allocator.get_allocation("unknown") == Decimal("0")

    def test_get_unallocated_empty(self):
        allocator = CapitalAllocator(Decimal("100000"))
        assert allocator.get_unallocated() == Decimal("100000")

    def test_get_available_reflects_usage(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("50000"))
        allocator.record_usage("strat_a", Decimal("20000"))
        assert allocator.get_available("strat_a") == Decimal("30000")


class TestBalanceUpdates:
    """Tests for update_total_balance."""

    def test_update_balance_increase(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.update_total_balance(Decimal("150000"))
        assert allocator.total_balance == Decimal("150000")
        assert allocator.get_unallocated() == Decimal("150000")

    def test_update_balance_decrease_allowed(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("30000"))
        allocator.update_total_balance(Decimal("50000"))
        assert allocator.get_unallocated() == Decimal("20000")

    def test_update_balance_below_allocated_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("60000"))
        with pytest.raises(ValueError, match="already allocated"):
            allocator.update_total_balance(Decimal("50000"))

    def test_update_balance_negative_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        with pytest.raises(ValueError, match="non-negative"):
            allocator.update_total_balance(Decimal("-1"))

    def test_update_balance_float_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        with pytest.raises(TypeError, match="Decimal"):
            allocator.update_total_balance(50000.0)  # type: ignore[arg-type]


class TestThreadSafety:
    """Basic thread safety tests for CapitalAllocator."""

    def test_concurrent_allocations(self):
        allocator = CapitalAllocator(Decimal("100000"))
        errors = []

        def allocate_strategy(sid: str, amount: Decimal):
            try:
                allocator.allocate(sid, amount)
            except ValueError:
                errors.append(sid)

        threads = []
        # 10 strategies each requesting 15000 — only ~6 can fit
        for i in range(10):
            t = threading.Thread(
                target=allocate_strategy,
                args=(f"strat_{i}", Decimal("15000")),
            )
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Sum of allocations + unallocated must equal total
        total_allocated = sum(
            allocator.get_allocation(f"strat_{i}") for i in range(10)
        )
        assert total_allocated + allocator.get_unallocated() == Decimal("100000")
        # At least some should have been rejected
        assert len(errors) > 0

    def test_concurrent_usage_recording(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("100000"))
        errors = []

        def record(amount: Decimal):
            try:
                allocator.record_usage("strat_a", amount)
            except ValueError:
                errors.append(True)

        threads = []
        # 20 threads each trying to record 10000 — only 10 should succeed
        for _ in range(20):
            t = threading.Thread(target=record, args=(Decimal("10000"),))
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        available = allocator.get_available("strat_a")
        assert available >= Decimal("0")
        assert available + Decimal(str(len(errors))) * Decimal("0") <= Decimal("100000")
        # Total used must be consistent
        used = Decimal("100000") - available
        successful = 20 - len(errors)
        assert used == Decimal(str(successful)) * Decimal("10000")


class TestEdgeCases:
    """Edge case tests."""

    def test_zero_balance_allocator(self):
        allocator = CapitalAllocator(Decimal("0"))
        with pytest.raises(ValueError, match="Cannot allocate"):
            allocator.allocate("strat_a", Decimal("1"))

    def test_zero_allocation_usage_raises(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("0"))
        with pytest.raises(ValueError, match="Cannot use"):
            allocator.record_usage("strat_a", Decimal("1"))

    def test_record_zero_usage(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("50000"))
        allocator.record_usage("strat_a", Decimal("0"))
        assert allocator.get_available("strat_a") == Decimal("50000")

    def test_release_zero_usage(self):
        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("strat_a", Decimal("50000"))
        allocator.release_usage("strat_a", Decimal("0"))
        assert allocator.get_available("strat_a") == Decimal("50000")

    def test_very_small_amounts(self):
        allocator = CapitalAllocator(Decimal("0.0001"))
        allocator.allocate("strat_a", Decimal("0.00005"))
        allocator.record_usage("strat_a", Decimal("0.00003"))
        assert allocator.get_available("strat_a") == Decimal("0.00002")

    def test_total_balance_property_is_readonly(self):
        allocator = CapitalAllocator(Decimal("100000"))
        # total_balance is a property, cannot be set directly
        with pytest.raises(AttributeError):
            allocator.total_balance = Decimal("50000")  # type: ignore[misc]
