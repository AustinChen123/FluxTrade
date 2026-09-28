"""Source decision replica, not a broker, matcher, backtest or deployable strategy.

External dictionaries intentionally retain the source program's field names.
All financial arithmetic is Decimal. See PROTOCOL.md for numeric deviations.
"""

from copy import deepcopy
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
from functools import wraps

D = Decimal
STATES = [
    "wait",
    "new",
    "sent",
    "live",
    "partially_filled",
    "canceling",
    "canceled",
    "mmp_canceled",
    "filled",
    "failed",
]
WORKING = {"sent", "live", "partially_filled"}
DEFAULTS = dict(
    TotalLimit="1",
    TotalLong="0.5",
    Clear="1",
    MaxTotalL="20",
    MinTotalL="5",
    CutLeverage="0.7",
    LeverageUp="0",
    defaultN="10",
    hlLimit="2",
    MinusStop="0.5",
    fixed="0",
    AvailRatioUp="1",
    AvailRatioDown="0",
)


def decimal_scope(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        with localcontext() as ctx:
            ctx.prec = 50
            return fn(*args, **kwargs)

    return wrapped


def num(value):
    if isinstance(value, float):
        raise TypeError("Float input is forbidden")
    return D(value)


def fmt(value):
    value = num(value)
    if not value.is_finite():
        raise ValueError("Unsupported non-finite source numerical path")
    result = format(value, "f")
    return result.rstrip("0").rstrip(".") if "." in result else result


def rank(value):
    return STATES.index(value) if value in STATES else -1


def seq_id(value):
    tail = value[-4:]
    try:
        return int(tail)
    except ValueError:
        return None


@decimal_scope
def seq_order(i, j, price, volume, step):
    p, v, r = num(price), num(volume), num(step)
    a = (1 + D("0.5") * r) / (1 + r)
    # Algebraic cancellation avoids subtracting a rounded reciprocal of a.
    # This is mathematical equivalence, not JavaScript Number parity.
    quantity = v * (1 - a) / a**i if j == -1 else v * (1 - a) * a ** (i - 1)
    return p * (1 + r) ** (i * j), quantity


class Policy:
    """Ordered source decisions with explicit I/O intents, no inferred fills."""

    def __init__(self, rows, markets, parameters=None, now_ms=100000):
        self.rows = deepcopy(rows)
        self.markets = deepcopy(markets)
        self.parameters = DEFAULTS | (parameters or {})
        self.capital = {}
        self.replies = {}
        self.running = False
        self.paused = False
        self.online = True
        self.ws_open = False
        self.now_ms = now_ms
        self.shared_ms = now_ms - 12000
        self.last_capital = None  # Source closure captures undefined at load time.
        self.last_market_ms = 0
        self.last_earn_ms = now_ms - 61000
        self.reset_ms = {}
        self.last_filled_price = {}
        self.order_id = 1
        self.events = []

    def emit(self, kind, **payload):
        self.events.append(dict(at_ms=self.now_ms, kind=kind, **deepcopy(payload)))

    def p(self, name):
        return num(self.parameters[name])

    def a(self, name):
        return num(self.capital[name])

    def row(self, name):
        return next((r for r in self.rows if r["name"] == name), None)

    @staticmethod
    def active(row):
        return str(row["active"]).lower() == "true"

    def request_market(self):
        if self.now_ms - self.last_market_ms < 2000:
            return
        self.last_market_ms = self.now_ms
        self.emit("request_market")  # Caller may deliver a later market snapshot.

    def apply_market(self, markets):
        self.markets = deepcopy(markets)

    @decimal_scope
    def apply_positions(self, positions):
        """Input is positions()'s signed notional dictionary, including other coins."""
        self.capital["position"] = sum((num(x) for x in positions.values()), D(0))
        for r in self.rows:
            r["hold"] = num(positions.get(r["name"], "0"))

    @decimal_scope
    def positions_response(self, data):
        positions = {}
        for comm in data:
            if comm["mgnMode"] != "cross":
                continue
            name = comm["instId"]
            m = self.markets.get(name, {})
            price = num(m.get("price", comm.get("last") or "0"))
            pos = num(comm["pos"])
            if price > 0:
                positions[name] = pos * price * num(m.get("ctVal", "1"))
            else:
                positions[name] = (
                    D(1) if pos > 0 else D(-1) if pos < 0 else D(0)
                ) * num(comm.get("notionalUsd") or "0")
        self.apply_positions(positions)

    @decimal_scope
    def raise_leverage(self):
        up = self.p("LeverageUp")
        if up == 0:
            return
        factor = (1 + up) ** (D(5) / D(86400))
        cap = self.p("MaxTotalL") / max(len(self.rows), 1)
        for r in self.rows:
            r["leverage"] = min(num(r["leverage"]) * factor, cap)

    @decimal_scope
    def make_order(self, name, suffix, side, price, quantity):
        m = self.markets.get(name)
        q = num(quantity)
        p = None if price == "" else num(price)
        code = 0
        if m:
            code = int(m.get("instIdCode") or 0)
            if p is not None:
                tick = num(m["increment"])
                rounding = ROUND_FLOOR if side == "buy" else ROUND_CEILING
                p = (p / tick).to_integral_value(rounding=rounding) * tick
            lot = num(m["lotSz"])
            q = (q / num(m["ctVal"]) / lot).to_integral_value(
                rounding=ROUND_FLOOR
            ) * lot
            if q < num(m["minSz"]):
                q = D(0)
        prefix = str(self.order_id % 1000000).zfill(6)
        self.order_id += 1
        return dict(
            instId=name,
            instIdCode=code,
            tdMode="cross",
            clOrdId=prefix + suffix,
            tag="cce88b5c6506BCDE",
            side=side,
            ordType="market" if p is None else "limit",
            px="" if p is None else fmt(p),
            sz=fmt(q),
        )

    def add_reply(self, od):
        key = od["clOrdId"]
        if key not in self.replies or rank(self.replies[key].get("state")) < rank(
            "new"
        ):
            self.replies[key] = {
                k: od[k] for k in ["instId", "side", "px", "sz", "clOrdId"]
            }
            self.replies[key]["state"] = "new"

    def transport(self, operation, ods):
        if not self.online:
            self.emit("alert", reason="offline", operation=operation)
            return
        unique = []
        seen = set()
        for od in ods:
            if not od or not od.get("instIdCode"):
                continue
            key = (od["instId"], od.get("side"), od.get("px"))
            if operation == "send" and key in seen:
                continue
            seen.add(key)
            unique.append(od)
        for offset in range(0, len(unique), 20):
            self.emit(
                operation if self.running else "console_only",
                operation=operation,
                route="WS" if self.ws_open else "REST",
                orders=unique[offset : offset + 20],
            )

    def cancel_order(self, ods):
        enriched = [
            od
            | {
                "instIdCode": int(
                    self.markets.get(od["instId"], {}).get("instIdCode") or 0
                )
            }
            for od in ods
        ]
        self.transport("cancel", enriched)

    def cancel_all(self, name):
        selected = [
            (key, od)
            for key, od in self.replies.items()
            if name == "All" or od["instId"] == name
        ]
        self.cancel_order(
            [
                dict(ordId=key, instId=od["instId"])
                for key, od in selected
                if od.get("state") in WORKING and od.get("clOrdId")
            ]
        )
        for key, od in selected:
            if od.get("state") not in WORKING:
                del self.replies[key]

    @decimal_scope
    def clear_inv(self, name):
        selected = [r for r in self.rows if name == "All" or r["name"] == name]
        if not selected:
            return
        self.request_market()
        ods = []
        for r in selected:
            price = num(self.markets.get(r["name"], {}).get("price") or "0")
            if not price:
                raise ValueError(
                    "Source clearInv would produce an undefined batch member"
                )
            deviation = num(r["hold"]) - self.a("total") * self.p("TotalLong") / len(
                self.rows
            )
            od = self.make_order(
                r["name"],
                "xxxx",
                "sell" if deviation > 0 else "buy",
                "",
                abs(deviation / price) * self.p("Clear"),
            )
            self.add_reply(od)
            ods.append(od)
        self.transport("send", ods)

    @decimal_scope
    def adjust_step(self, r):
        m = self.markets.get(r["name"])
        step = num(r["歩差"])
        if m:
            q = (
                self.a("total")
                * num(r["leverage"])
                * D("0.25")
                * step
                / (1 + step)
                / num(m["price"])
                / num(m["ctVal"])
            )
            if q < num(m["minSz"]):
                step *= num(m["minSz"]) / q * 2
        return step

    def reset_strategy(self, name):
        if self.now_ms - self.reset_ms.get(name, 0) <= 3000:
            return
        self.reset_ms[name] = self.now_ms
        r = self.row(name)
        if r:
            r["歩差"] = self.adjust_step(r)
            r["單數"] = "1"
            self.emit("alert", reason="reset_step", name=name)
            self.start_strategy(name)

    @decimal_scope
    def start_strategy(self, name):
        r = self.row(name)
        if not self.running or not r or not self.active(r):
            return
        self.cancel_all(name)
        self.request_market()
        m = self.markets.get(name)
        if not m or m["state"] != "live" or num(m["ratioHL"]) > self.p("hlLimit"):
            return
        p, step = num(m["price"]), num(r["歩差"])
        index = int(
            (p.ln() / (1 + step).ln() + D("0.5")).to_integral_value(
                rounding=ROUND_FLOOR
            )
        )
        volume = self.a("total") * num(r["leverage"]) * D("0.5") / p
        ods = []
        for i in range(1, int(r["單數"]) + 1):
            for j in [-1, 1]:
                px, q = seq_order(i, j, p, volume, step)
                od = self.make_order(
                    name,
                    str(index + i * j + 5000).rjust(4, "0"),
                    "buy" if j == -1 else "sell",
                    px,
                    q,
                )
                if not any(
                    (x["instId"], x["side"], x["px"])
                    == (od["instId"], od["side"], od["px"])
                    for x in ods
                ):
                    self.add_reply(od)
                    ods.append(od)
        if any(od["sz"] == "0" for od in ods):
            self.reset_strategy(name)
        else:
            self.transport("send", ods)

    @decimal_scope
    def order_filled(self, od):
        if len(od["clOrdId"]) < 10:
            return
        r = self.row(od["instId"])
        last = self.last_filled_price.setdefault(od["instId"], dict(buy="0", sell="0"))
        if not r or not self.active(r) or not self.running:
            return
        index = seq_id(od["clOrdId"])
        if index is None or od["px"] == "":
            self.emit(
                "unsupported_source_path",
                reason="non_grid_fill_enters_orderFilled",
                order=od,
            )
            return  # Source continues with NaN; not silently treated as a valid grid.
        n = int(r["單數"])
        targets = (
            [index + n + 1, index - n, index + 1]
            if od["side"] == "buy"
            else [index - n - 1, index + n, index - 1]
        )
        self.cancel_order(
            [
                dict(ordId=key, instId=x["instId"])
                for key, x in self.replies.items()
                if x["instId"] == od["instId"]
                and len(x["clOrdId"]) >= 10
                and seq_id(x["clOrdId"]) in targets
            ]
        )
        price = num(od["px"])  # Original order limit, not fillPx or avgPx.
        volume = self.a("total") * num(r["leverage"]) * D("0.5") / price
        ods = []
        for j in [-1, 1]:
            side = "buy" if j == -1 else "sell"
            i = n if side == od["side"] else 1
            px, q = seq_order(i, j, price, volume, num(r["歩差"]))
            new = self.make_order(
                od["instId"], str(index + i * j).rjust(4, "0"), side, px, q
            )
            if last[side] != new["px"]:
                self.add_reply(new)
                last[side] = new["px"]
                ods.append(new)
        if any(x["sz"] == "0" for x in ods):
            self.reset_strategy(od["instId"])
        else:
            self.transport("send", ods)

    @decimal_scope
    def orders(self, od):
        key, client = od["ordId"], od["clOrdId"]
        if client in self.replies and key not in self.replies:
            self.replies[key] = self.replies.pop(client)
        row = self.replies.setdefault(key, {})
        row.update({k: od[k] for k in ["instId", "side", "px", "clOrdId"]})
        fill = num(od.get("fillPx") or "0")
        if (
            od["state"] == "filled"
            and fill.is_finite()
            and fill > 0
            and od["instId"] in self.markets
        ):
            self.markets[od["instId"]]["price"] = fill
        if max(0, rank(row.get("state"))) < max(0, rank(od["state"])):
            row["state"] = od["state"]
        ct = num(self.markets.get(od["instId"], {}).get("ctVal", "1"))
        row["sz"], row["accFillSz"] = (
            fmt(num(od["sz"]) * ct),
            fmt(num(od["accFillSz"]) * ct),
        )
        row["cTime"] = od.get("cTime")  # Display formatting is not a decision input.
        if row.get("state") in ["canceled", "mmp_canceled"]:
            del self.replies[key]
        elif row.get("state") == "filled":
            self.order_filled(row)
            self.replies.pop(key, None)

    @decimal_scope
    def open_orders(self, ods):
        self.replies = {}
        for od in ods:
            ct = num(self.markets.get(od["instId"], {}).get("ctVal", "1"))
            self.replies[od["ordId"]] = {
                k: od[k] for k in ["clOrdId", "instId", "state", "side", "px"]
            }
            self.replies[od["ordId"]].update(
                sz=fmt(num(od["sz"]) * ct),
                accFillSz=fmt(num(od["accFillSz"]) * ct),
                cTime=od.get("cTime"),
            )
        self.compare_reply()

    def compare_reply(self):
        snapshot = list(self.replies.values())
        for r in self.rows:
            count = sum(
                1 for od in snapshot if od["instId"] == r["name"] and od.get("clOrdId")
            )
            if count != int(r["單數"]) * 2:
                self.start_strategy(r["name"])

    @decimal_scope
    def cut(self, row):
        row["leverage"] = max(
            self.p("MinTotalL") / len(self.rows),
            num(row["leverage"]) * self.p("CutLeverage"),
        )

    def total_limit_move(self):
        if self.now_ms - self.shared_ms <= 11000:
            return
        self.shared_ms = self.now_ms
        self.clear_inv("All")
        for r in self.rows:
            self.cut(r)
            self.start_strategy(r["name"])
        self.last_capital = self.a("total")
        self.emit("alert", reason="total_limit")

    @decimal_scope
    def check_single(self, r):
        ratio = num(self.markets.get(r["name"], {}).get("ratioHL", "0"))
        active_reply = any(
            x["instId"] == r["name"] and len(x["clOrdId"]) >= 10
            for x in self.replies.values()
        )
        if ratio > self.p("hlLimit") and active_reply:
            self.cancel_all(r["name"])
            self.emit("alert", reason="high_low_limit", name=r["name"])
        # The source's restart-in-range branch is commented out.
        hold = num(r["hold"]) / self.a("total")
        if (
            hold > num(r["hold上限"]) or hold < num(r["hold下限"])
        ) and self.now_ms - self.shared_ms > 11000:
            self.cut(r)
            self.clear_inv(r["name"])
            self.start_strategy(r["name"])
            self.emit("alert", reason="individual_limit", name=r["name"])
            self.shared_ms = self.now_ms

    @decimal_scope
    def check_risk(self):
        if self.a("usdt") <= 0 or not self.rows:
            return
        if abs(self.a("position")) / self.a("total") > self.p("TotalLimit"):
            self.total_limit_move()
        else:
            for r in self.rows:
                self.check_single(r)
        day = self.capital.get("day_ago")
        if day is not None and self.a("total") / num(day) < self.p("MinusStop"):
            self.cancel_all("All")
            self.running = False
            self.emit("alert", reason="daily_stop")
        if self.now_ms - self.shared_ms > 21000 and self.last_capital is not None:
            threshold = max(D("0.05"), self.a("position") / self.a("total") * D("0.1"))
            if abs(self.a("total") / self.last_capital - 1) > threshold:
                self.last_capital = self.a("total")
                self.shared_ms = self.now_ms
                for r in self.rows:
                    self.start_strategy(r["name"])
        # checkFinance is not invoked by the source.

    @decimal_scope
    def timer_action(
        self,
        now_ms,
        *,
        earn=None,
        trading=None,
        positions=None,
        open_orders=None,
        local_hour=None,
        local_minute=None,
    ):
        """One completed timer invocation; None responses mean failure/unchanged.

        Snapshot delivery interleavings use the individual methods instead. Worker
        scheduling is external. Each call grows by five seconds regardless of gap.
        """
        if self.paused:
            return
        self.now_ms = now_ms
        if now_ms - self.last_earn_ms > 60000:
            self.last_earn_ms = now_ms
            self.emit("request_earn")
            if earn is not None:
                self.capital["earn"] = num(earn)
        if trading is not None:
            self.capital.update(usdt=num(trading["eq"]), avail=num(trading["availEq"]))
            if "earn" not in self.capital:
                raise ValueError("Source total is NaN before an earn snapshot exists")
            self.capital["total"] = self.a("usdt") + self.a("earn") - self.p("fixed")
        if positions is not None:
            self.apply_positions(positions)
        if open_orders is not None:
            self.open_orders(open_orders)
        self.raise_leverage()
        if self.running and not self.ws_open:
            self.emit("check_websocket")
        self.check_risk()
        if local_hour == 12 and local_minute == 0:
            self.capital["day_ago"] = self.a("total")

    def rest_ack(self, operation, entries):
        """postOrder/postCancel per-item acknowledgments; WS acks are ignored."""
        for od in entries:
            client, real = od.get("clOrdId"), od.get("ordId")
            ok = str(od["sCode"]) == "0"
            if operation == "send" and ok:
                if client in self.replies:
                    self.replies[real] = self.replies.pop(client)
                    if rank(self.replies[real].get("state")) < rank("sent"):
                        self.replies[real]["state"] = "sent"
                else:
                    self.replies[real] = {
                        k: od.get(k) for k in ["instId", "side", "px", "sz"]
                    }
                    self.replies[real].update(state="sent", clOrdId=client)
            else:
                key = (
                    (real if real is not None else client)
                    if operation == "cancel"
                    else (
                        client
                        if client in self.replies
                        else real
                        if real in self.replies
                        else client or real
                    )
                )
                if not key:
                    continue
                row = self.replies.setdefault(key, {})
                if ok:
                    if rank(row.get("state")) < rank("canceling"):
                        row["state"] = "canceling"
                else:
                    row.update(state="failed", sMsg=od.get("sMsg"))
