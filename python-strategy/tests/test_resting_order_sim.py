import ast
import math
from collections.abc import Callable
from pathlib import Path

import pytest

from core.resting_order_sim import SimRecord, simulate
from core.resting_order_strategy import (
    DesiredOrder,
    MinuteBar,
    RestingFill,
    RestingIntent,
    RestingOrderStrategy,
    Side,
)


class Ladder(RestingOrderStrategy):
    def __init__(self) -> None:
        self.fills: list[RestingFill] = []
        self.resting: dict[tuple[Side, float], float] = {("buy", 99): 1, ("buy", 98): 1}

    def _intent(self, flatten: bool = False) -> RestingIntent:
        return RestingIntent(
            tuple(
                DesiredOrder(side, price, qty)
                for (side, price), qty in sorted(self.resting.items())
            ),
            flatten=flatten,
        )

    def on_bar(self, bar: MinuteBar) -> RestingIntent:
        return self._intent(flatten=bar.ts_ms == 180_000)

    def on_fill(self, fill: RestingFill) -> RestingIntent:
        self.fills.append(fill)
        del self.resting[(fill.side, fill.price)]
        if fill.side == "buy":
            self.resting[("sell", fill.price + 2)] = 1
        else:
            self.resting[("buy", fill.price - 2)] = 1
        return self._intent()


def bars(closes: list[float]) -> list[MinuteBar]:
    return [
        MinuteBar(i * 60_000, close, close, close, close)
        for i, close in enumerate(closes)
    ]


def select(records: list[SimRecord], kind: str) -> list[SimRecord]:
    return [record for record in records if record.kind == kind]


def test_r1_t1_ladder_oracle() -> None:
    strategy = Ladder()
    """Gross is -99-98+100+101-99+99=4. Fees are .0002*(99+98+100+101+99)+.0005*99=.1489."""
    records = simulate(strategy, bars([100, 98, 101, 99]), 0.0002, 0.0005)
    fills = select(records, "fill")
    assert [(r.ts_ms, r.side, r.price, r.quantity) for r in fills] == [
        (60_000, "buy", 99, 1),
        (60_000, "buy", 98, 1),
        (120_000, "sell", 100, 1),
        (120_000, "sell", 101, 1),
        (180_000, "buy", 99, 1),
    ]
    assert [r.fee for r in fills] == pytest.approx(
        [0.0198, 0.0196, 0.02, 0.0202, 0.0198], abs=1e-12
    )
    assert [(f.side, f.price, f.quantity) for f in strategy.fills] == [
        (r.side, r.price, r.quantity) for r in fills
    ]
    settlements = select(records, "settle")
    assert [(r.gross, r.fee) for r in settlements] == pytest.approx(
        [(4.0, 0.1489)], abs=1e-12
    )
    flatten = select(records, "flatten")
    assert [(r.ts_ms, r.quantity, r.price, r.fee) for r in flatten] == [
        (180_000, 1, 99, pytest.approx(0.0495))
    ]
    assert [(r.side, r.price) for r in strategy.fills[-1:]] == [("buy", 99)]
    assert select(records, "bar")[-1].ts_ms == 180_000


class MultiFill(RestingOrderStrategy):
    def __init__(self) -> None:
        self.calls: list[str] = []

    def on_bar(self, bar: MinuteBar) -> RestingIntent:
        self.calls.append("bar")
        return (
            RestingIntent((DesiredOrder("buy", 99, 1), DesiredOrder("buy", 98, 1)))
            if bar.ts_ms == 0
            else RestingIntent(())
        )

    def on_fill(self, fill: RestingFill) -> RestingIntent:
        self.calls.append(f"fill:{fill.price}")
        if fill.price == 99:
            return RestingIntent(
                (DesiredOrder("buy", 98, 1), DesiredOrder("buy", 97, 1))
            )
        if fill.price == 98:
            return RestingIntent((DesiredOrder("buy", 97, 1),))
        return RestingIntent(())


def test_r1_t2_multi_fill_same_close_highest_first() -> None:
    strategy = MultiFill()
    records = simulate(strategy, bars([100, 96]), 0, 0)
    assert [(r.ts_ms, r.side, r.price) for r in select(records, "fill")] == [
        (60_000, "buy", 99),
        (60_000, "buy", 98),
        (60_000, "buy", 97),
    ]
    assert strategy.calls == ["bar", "fill:99", "fill:98", "fill:97", "bar"]


@pytest.mark.parametrize(
    ("side", "price", "equal", "beyond"),
    [
        ("buy", 99.0, 99.0, math.nextafter(99.0, math.inf)),
        ("sell", 101.0, 101.0, math.nextafter(101.0, -math.inf)),
    ],
)
def test_r1_t3_inclusive_close_boundary_and_ulp_twin(
    side: Side, price: float, equal: float, beyond: float
) -> None:
    class One(RestingOrderStrategy):
        def on_bar(self, bar: MinuteBar) -> RestingIntent:
            return (
                RestingIntent((DesiredOrder(side, price, 1),))
                if bar.ts_ms == 0
                else RestingIntent(())
            )

        def on_fill(self, fill: RestingFill) -> RestingIntent:
            return RestingIntent(())

    baseline = simulate(
        One(),
        [
            MinuteBar(0, equal, max(100, equal), min(100, equal), equal),
            MinuteBar(60_000, equal, max(100, equal), min(100, equal), equal),
        ],
        0,
        0,
    )
    assert len(select(baseline, "fill")) == 1
    missed = simulate(
        One(),
        [
            MinuteBar(0, equal, max(100, equal), min(100, equal), equal),
            MinuteBar(60_000, beyond, max(100, beyond), min(100, beyond), beyond),
        ],
        0,
        0,
    )
    assert select(missed, "fill") == []


def test_r1_t4_matching_precedes_bar_callback_and_new_orders_wait() -> None:
    class OrderLog(RestingOrderStrategy):
        def __init__(self) -> None:
            self.events: list[str] = []

        def on_bar(self, bar: MinuteBar) -> RestingIntent:
            self.events.append(f"bar:{bar.ts_ms}")
            return (
                RestingIntent((DesiredOrder("buy", 99, 1),))
                if bar.ts_ms == 0
                else RestingIntent(())
            )

        def on_fill(self, fill: RestingFill) -> RestingIntent:
            self.events.append(f"fill:{fill.ts_ms}")
            return RestingIntent(())

    strategy = OrderLog()
    records = simulate(
        strategy,
        [MinuteBar(0, 100, 100, 100, 100), MinuteBar(60_000, 99, 100, 99, 99)],
        0,
        0,
    )
    assert strategy.events == ["bar:0", "fill:60000", "bar:60000"]
    assert [(r.ts_ms, r.kind) for r in records if r.kind == "fill"] == [
        (60_000, "fill")
    ]


def test_r1_t5_long_short_and_zero_position_settlement() -> None:
    class Short(RestingOrderStrategy):
        def on_bar(self, bar: MinuteBar) -> RestingIntent:
            if bar.ts_ms == 0:
                return RestingIntent((DesiredOrder("sell", 99, 2),))
            return (
                RestingIntent((), flatten=True)
                if bar.ts_ms == 120_000
                else RestingIntent((DesiredOrder("sell", 99, 2),))
            )

        def on_fill(self, fill: RestingFill) -> RestingIntent:
            return RestingIntent(())

    records = simulate(
        Short(),
        [
            MinuteBar(0, 100, 100, 100, 100),
            MinuteBar(60_000, 99, 99, 99, 99),
            MinuteBar(120_000, 90, 90, 90, 90),
        ],
        0,
        0.0005,
    )
    assert [(r.quantity, r.price, r.fee) for r in select(records, "flatten")] == [
        (2, 90, 0.09)
    ]
    settles = select(records, "settle")
    assert [(r.gross, r.fee) for r in settles] == [(18, 0.09)]


class SequenceStrategy(RestingOrderStrategy):
    def __init__(self, intents: list[RestingIntent]) -> None:
        self.intents = iter(intents)

    def on_bar(self, bar: MinuteBar) -> RestingIntent:
        return next(self.intents, RestingIntent(()))

    def on_fill(self, fill: RestingFill) -> RestingIntent:
        return RestingIntent(())


def test_r1_t5_long_flatten_and_zero_position_settle_resets() -> None:
    strategy = SequenceStrategy(
        [
            RestingIntent((DesiredOrder("buy", 100, 2),)),
            RestingIntent((), flatten=True),
            RestingIntent((), flatten=True),
        ]
    )
    records = simulate(
        strategy,
        [
            MinuteBar(0, 100, 100, 100, 100),
            MinuteBar(60_000, 99, 100, 99, 99),
            MinuteBar(120_000, 100, 100, 100, 100),
            MinuteBar(180_000, 100, 100, 100, 100),
        ],
        0,
        0.0005,
    )
    assert [(r.quantity, r.price, r.fee) for r in select(records, "flatten")] == [
        (2, 99, 0.099)
    ]
    assert [(r.gross, r.fee) for r in select(records, "settle")] == [
        (-2, 0.099),
        (0, 0),
    ]


def test_r1_t7_touch_without_close_fill() -> None:
    class Touch(RestingOrderStrategy):
        def on_bar(self, bar: MinuteBar) -> RestingIntent:
            return (
                RestingIntent((DesiredOrder("buy", 99, 1),))
                if bar.ts_ms == 0
                else RestingIntent(())
            )

        def on_fill(self, fill: RestingFill) -> RestingIntent:
            return RestingIntent(())

    records = simulate(
        Touch(),
        [MinuteBar(0, 100, 100, 100, 100), MinuteBar(60_000, 100, 100, 99, 100)],
        0,
        0,
    )
    diagnostic = select(records, "bar")[1]
    assert diagnostic.touch_buy is True
    assert select(records, "fill") == []


@pytest.mark.parametrize(
    "make_bad",
    [
        lambda: RestingIntent((DesiredOrder("buy", 99, 1), DesiredOrder("buy", 99, 2))),
        lambda: RestingIntent(
            (DesiredOrder("buy", 100, 1), DesiredOrder("sell", 100, 1))
        ),
        lambda: RestingIntent([DesiredOrder("buy", 99, 1)]),  # pyright: ignore[reportArgumentType]
        lambda: RestingIntent((object(),)),  # pyright: ignore[reportArgumentType]
        lambda: DesiredOrder("hold", 99, 1),  # pyright: ignore[reportArgumentType]
        lambda: RestingFill(0, "hold", 99, 1),  # pyright: ignore[reportArgumentType]
        lambda: MinuteBar(True, 1, 1, 1, 1),
        lambda: MinuteBar(0, True, True, True, True),
        lambda: MinuteBar(1, 1, 1, 1, 1),
        lambda: MinuteBar(0, 10, 9, 8, 10),
        lambda: MinuteBar(0, 10, 11, 8, 12),
        lambda: RestingIntent((), flatten=1),  # pyright: ignore[reportArgumentType]
    ],
)
def test_r1_t6_rejects_invalid_records_with_legal_twins(
    make_bad: Callable[[], object],
) -> None:
    with pytest.raises(ValueError):
        make_bad()
    assert MinuteBar(0, 10, 11, 9, 10)
    assert RestingIntent((DesiredOrder("buy", 99, 1), DesiredOrder("sell", 101, 1)))


@pytest.mark.parametrize("field", ["price", "quantity"])
@pytest.mark.parametrize("value", [0.0, -1.0, math.nan, math.inf, -math.inf, True, "1"])
def test_r1_t6_order_finite_positive_fields(field: str, value: object) -> None:
    values: dict[str, object] = {"side": "buy", "price": 99.0, "quantity": 1.0}
    values[field] = value
    with pytest.raises(ValueError):
        DesiredOrder(**values)  # pyright: ignore[reportArgumentType]
    with pytest.raises(ValueError):
        RestingFill(0, "buy", **{k: v for k, v in values.items() if k != "side"})  # pyright: ignore[reportArgumentType]
    values[field] = 99 if field == "price" else 1
    assert DesiredOrder(**values)  # pyright: ignore[reportArgumentType]
    assert RestingFill(0, "buy", 99, 1)


@pytest.mark.parametrize(
    "maker,taker",
    [(-0.1, 0), (0, -0.1), (math.nan, 0), (0, math.inf), (True, 0), (0, "0")],
)
def test_r1_t6_rejects_bad_fees(maker: float, taker: float) -> None:
    with pytest.raises(ValueError):
        simulate(SequenceStrategy([]), [], maker, taker)
    assert simulate(SequenceStrategy([]), [], 0, 0) == []


def test_r1_t6_nonincreasing_timestamp() -> None:
    with pytest.raises(ValueError):
        simulate(
            SequenceStrategy([RestingIntent(()), RestingIntent(())]),
            [MinuteBar(60_000, 1, 1, 1, 1), MinuteBar(60_000, 1, 1, 1, 1)],
            0,
            0,
        )
    assert simulate(
        SequenceStrategy([RestingIntent(()), RestingIntent(())]),
        [MinuteBar(0, 1, 1, 1, 1), MinuteBar(60_000, 1, 1, 1, 1)],
        0,
        0,
    )


@pytest.mark.parametrize("fills", [1000, 1001])
def test_r1_t6_fill_cap_boundary(fills: int) -> None:
    class Replacing(RestingOrderStrategy):
        def __init__(self) -> None:
            self.count = 0

        def on_bar(self, bar: MinuteBar) -> RestingIntent:
            return RestingIntent(
                (DesiredOrder("buy", 100, 1),) if bar.ts_ms == 0 else ()
            )

        def on_fill(self, fill: RestingFill) -> RestingIntent:
            self.count += 1
            return RestingIntent(
                (DesiredOrder("buy", 100, 1),) if self.count < fills else ()
            )

    bars = [MinuteBar(0, 100, 100, 100, 100), MinuteBar(60_000, 99, 100, 99, 99)]
    if fills > 1000:
        with pytest.raises(ValueError, match="1000 fills"):
            simulate(Replacing(), bars, 0, 0)
    else:
        assert len(select(simulate(Replacing(), bars, 0, 0), "fill")) == fills


def test_r1_t6_non_intent_return_rejected() -> None:
    class Broken(SequenceStrategy):
        def on_bar(self, bar: MinuteBar) -> RestingIntent:
            return None  # pyright: ignore[reportReturnType]

    with pytest.raises(ValueError, match="RestingIntent"):
        simulate(Broken([]), [MinuteBar(0, 100, 100, 100, 100)], 0, 0)
    assert simulate(SequenceStrategy([]), [MinuteBar(0, 100, 100, 100, 100)], 0, 0)


def test_r1_t8_import_and_environment_isolation() -> None:
    root = Path(__file__).parents[1] / "src"
    stdlib = set(__import__("sys").stdlib_module_names)
    allowed_project = {
        "core/resting_order_strategy.py": set(),
        "core/resting_order_sim.py": {"core.resting_order_strategy"},
    }
    for name, project_modules in allowed_project.items():
        tree = ast.parse((root / name).read_text())
        aliases: dict[str, str] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    aliases[alias.asname or alias.name.split(".")[0]] = alias.name
            elif isinstance(node, ast.ImportFrom) and node.module:
                aliases[node.module.split(".")[0]] = node.module
                for alias in node.names:
                    aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules = [node.module] if node.module else []
            else:
                modules = []
            assert all(
                module.split(".")[0] in stdlib or module in project_modules
                for module in modules
            ), (name, modules)
            if isinstance(node, ast.Attribute):
                current: ast.expr = node
                attrs: list[str] = []
                while isinstance(current, ast.Attribute):
                    attrs.append(current.attr)
                    current = current.value
                if isinstance(current, ast.Name):
                    qualified = (
                        aliases.get(current.id, current.id)
                        + "."
                        + ".".join(reversed(attrs))
                    )
                    assert qualified not in {
                        "os.environ",
                        "os.getenv",
                        "os.environ.get",
                    }
            if isinstance(node, ast.Name):
                assert node.id not in {"getenv", "environ"}
            if isinstance(node, ast.Call):
                assert not (
                    isinstance(node.func, ast.Name)
                    and aliases.get(node.func.id, "").startswith("ccxt")
                )
