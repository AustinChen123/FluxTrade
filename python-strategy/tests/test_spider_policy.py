"""Deterministic decision fixtures; no matching or performance ledger."""

import ast
from pathlib import Path
from typing import cast
import unittest
from src.core.backtest.spider_policy import D, Policy, seq_order

POLICY_PATH = Path(__file__).parents[1] / "src/core/backtest/spider_policy.py"


def fixture(n=2, **params):
    rows = [
        dict(
            name=f"C{i}",
            active="true",
            leverage="4",
            歩差="0.01",
            單數="2",
            hold上限="0.8",
            hold下限="-0.8",
            hold="0",
        )
        for i in range(n)
    ]
    markets = {
        r["name"]: dict(
            price="100",
            ctVal="1",
            lotSz="0.001",
            minSz="0.001",
            increment="0.01",
            ratioHL="0.1",
            state="live",
            instIdCode=i + 1,
        )
        for i, r in enumerate(rows)
    }
    p = Policy(rows, markets, params)
    p.capital = dict(total="10000", usdt="10000", earn="0", avail="10000", position="0")
    p.running = True
    return p


def sent(p, kind="send"):
    return [o for e in p.events if e["kind"] == kind for o in e["orders"]]


def report(o, state="filled", **kw):
    return (
        dict(
            o, ordId="ex1", state=state, fillPx="99", accFillSz=o["sz"], cTime="100000"
        )
        | kw
    )


class Tests(unittest.TestCase):
    def test_provenance_identity_not_source_byte_revalidation(self):
        """Frozen provenance identities only; CI does not possess source bytes."""
        provenance = {
            "source_manifest.json": "eb6ab34d8685fb59e286f5ffda8af24cbecf3e5ab2c729c585ccdca797cac336",
            "strategy.js": "0c2481f5ee8e55a82a99bce99b34ff3605029659ea35114010bc4b6402e9d05a",
            "okx.js": "c4fd739a04b9b2cd25cf61723242ea526b0ae3af3284f5af328a56c76395e543",
            "rwInfo.js": "3683dc2398a708724b8a388d209927aa68d48a2b6481d400f5558aaf20329655",
        }
        self.assertEqual(set(provenance), {"source_manifest.json", "strategy.js", "okx.js", "rwInfo.js"})
        for identity in provenance.values():
            self.assertRegex(identity, r"^[0-9a-f]{64}$", "provenance identity, not source-byte verification")

    def test_no_float_network(self):
        tree = ast.parse(POLICY_PATH.read_text())
        self.assertFalse(
            any(
                isinstance(x, ast.Constant) and isinstance(x.value, float)
                for x in ast.walk(tree)
            )
        )
        self.assertEqual(
            {x.module for x in ast.walk(tree) if isinstance(x, ast.ImportFrom)},
            {"copy", "decimal", "functools"},
        )
        with self.assertRaises(TypeError):
            seq_order(1, 1, 1.0, "1", "0.1")

    def test_ladder(self):
        self.assertEqual(seq_order(1, -1, "100", "300", "1"), (D(50), D(100)))
        px, q = seq_order(1, 1, "100", "300", "1")
        self.assertEqual(px, D(200))
        self.assertEqual(q, D(75))
        self.assertEqual(fixture().make_order("C0", "1", "sell", px, q)["sz"], "75")

    def test_start_matrix(self):
        for running in [False, True]:
            for active in ["false", "true"]:
                for state in ["live", "suspend", None]:
                    with self.subTest(running=running, active=active, state=state):
                        p = fixture()
                        p.running = running
                        p.rows[0]["active"] = active
                        if state is None:
                            del p.markets["C0"]
                        else:
                            p.markets["C0"]["state"] = state
                        p.start_strategy("C0")
                        self.assertEqual(
                            len(sent(p)),
                            4
                            if running and active == "true" and state == "live"
                            else 0,
                        )

    def test_rounding_minimum(self):
        p = fixture()
        for side, expected in [("buy", "100"), ("sell", "100.01")]:
            self.assertEqual(
                p.make_order("C0", "1", side, "100.001", "0.001")["px"], expected
            )
        for q, expected in [
            ("0.000999", "0"),
            ("0.001", "0.001"),
            ("0.001999", "0.001"),
        ]:
            self.assertEqual(p.make_order("C0", "1", "buy", "", q)["sz"], expected)
        p.markets["C0"]["ctVal"] = "0.01"
        self.assertEqual(p.make_order("C0", "1", "buy", "", "1")["sz"], "100")

    def test_h07_normalization_dedup_minimum_reset_and_new_tick(self):
        p = fixture(n=1)
        p.markets["C0"].update(price="100", increment=D("1"), lotSz=D("1"), minSz=D("1"))
        long_1039 = p.make_order("C0", "H071", "buy", "103.9", "1")
        long_1031 = p.make_order("C0", "H072", "buy", "103.1", "1")
        self.assertEqual((long_1039["px"], long_1031["px"]), ("103", "103"))
        p.transport("send", [long_1039, long_1031])
        self.assertEqual([row["clOrdId"] for row in sent(p)], [long_1039["clOrdId"]])

        p.events.clear()
        p.rows[0].update({"歩差": "1", "單數": "1"})
        p.capital.update(total="75", usdt="75", avail="75")
        before_capital = p.capital.copy()
        raw_orders = []
        make_order = p.make_order

        def capture_raw_order(name, suffix, side, price, quantity):
            raw_orders.append((side, D(quantity)))
            return make_order(name, suffix, side, price, quantity)

        p.make_order = capture_raw_order
        p.start_strategy("C0")
        self.assertIn(("buy", D("0.5")), raw_orders)
        self.assertNotEqual(p.rows[0]["歩差"], D("1"))
        self.assertEqual(p.make_order("C0", "H073", "buy", "", "0.5")["sz"], "0")
        self.assertIn(dict(at_ms=p.now_ms, kind="alert", reason="reset_step", name="C0"), p.events)
        self.assertFalse(sent(p))
        self.assertEqual(p.capital, before_capital)

        p.markets["C0"]["increment"] = D("5")
        self.assertEqual(p.make_order("C0", "H074", "buy", "103", "1")["px"], "100")

    def test_raise_zero_cap_inactive(self):
        p = fixture()
        p.rows[0]["leverage"] = "30"
        p.raise_leverage()
        self.assertEqual(p.rows[0]["leverage"], "30")
        p.parameters["LeverageUp"] = "0.1"
        p.rows[1]["active"] = "false"
        p.raise_leverage()
        self.assertEqual(p.rows[0]["leverage"], D(10))
        self.assertGreater(cast(D, p.rows[1]["leverage"]), D(4))
        p.rows[0]["leverage"] = "0"
        p.raise_leverage()
        self.assertEqual(p.rows[0]["leverage"], 0)
        p.rows[0]["leverage"] = "10"
        p.raise_leverage()
        self.assertEqual(p.rows[0]["leverage"], 10)

    def test_growth_is_per_call(self):
        p, q = fixture(LeverageUp="0.1"), fixture(LeverageUp="0.1")
        p.timer_action(105000)
        q.timer_action(9999999999)
        self.assertEqual(p.rows, q.rows)

    def test_global_threshold_cooldown(self):
        for pos in ["9999", "10000", "10001", "-10001"]:
            for elapsed in [10999, 11000, 11001]:
                with self.subTest(pos=pos, elapsed=elapsed):
                    p = fixture()
                    p.capital["position"] = pos
                    p.shared_ms = p.now_ms - elapsed
                    p.check_risk()
                    trigger = abs(D(pos)) > 10000 and elapsed > 11000
                    self.assertEqual(
                        p.rows[0]["leverage"], D("2.8") if trigger else "4"
                    )

    def test_clear_center_partial_and_async(self):
        for clear, expected in [("1", ["55", "15"]), ("0.5", ["27.5", "7.5"])]:
            p = fixture(Clear=clear)
            p.apply_positions({"C0": "8000", "C1": "4000"})
            p.check_risk()
            market = [o for o in sent(p) if o["ordType"] == "market"]
            self.assertEqual([o["sz"] for o in market], expected)
            self.assertEqual([o["side"] for o in market], ["sell", "sell"])
            self.assertEqual([r["hold"] for r in p.rows], [D(8000), D(4000)])
            self.assertEqual([r["leverage"] for r in p.rows], [D("2.8"), D("2.8")])
            self.assertEqual(sent(p)[0]["ordType"], "market")
            self.assertEqual(len(sent(p)), 10)

    def test_inactive_and_floor(self):
        p = fixture()
        p.rows[0].update(active="false", leverage="0")
        p.capital["position"] = "11000"
        p.check_risk()
        self.assertEqual(p.rows[0]["leverage"], D("2.5"))
        self.assertEqual(len([o for o in sent(p) if o["ordType"] == "market"]), 2)
        self.assertEqual(len([o for o in sent(p) if o["ordType"] == "limit"]), 4)

    def test_net_and_outside_positions(self):
        p = fixture()
        for r in p.rows:
            r.update(hold上限="3", hold下限="-3")
        p.apply_positions({"C0": "20000", "C1": "-20000"})
        p.check_risk()
        self.assertEqual(sent(p), [])
        p.apply_positions({"OUTSIDE": "11000"})
        p.check_risk()
        self.assertTrue(any(e.get("reason") == "total_limit" for e in p.events))
        self.assertFalse(any(o["instId"] == "OUTSIDE" for o in sent(p)))

    def test_individual_order_and_global_priority(self):
        for hold in ["9000", "-9000"]:
            p = fixture()
            for r in p.rows:
                r["hold"] = hold
            p.check_risk()
            self.assertEqual([r["leverage"] for r in p.rows], [D("2.8"), "4"])
            self.assertIsNone(p.last_capital)
        for hold in ["8000", "-8000"]:
            p = fixture()
            p.rows[0]["hold"] = hold
            p.check_risk()
            self.assertEqual(sent(p), [])
        p = fixture()
        p.rows[0]["hold"] = "9000"
        p.capital["position"] = "11000"
        p.shared_ms = p.now_ms
        p.check_risk()
        self.assertEqual(sent(p), [])

    def test_capital_reference(self):
        p = fixture()
        p.now_ms += 50000
        p.capital["total"] = "20000"
        p.check_risk()
        self.assertEqual(sent(p), [])
        for position, expected in [("-9000", 8), ("9000", 0)]:
            p = fixture()
            p.last_capital = D(9400)
            p.now_ms += 22000
            p.capital["position"] = position
            p.check_risk()
            self.assertEqual(len(sent(p)), expected)

    def test_daily_stop_and_noon(self):
        p = fixture()
        p.check_risk()
        self.assertTrue(p.running)
        p.capital["day_ago"] = "20000"
        p.check_risk()
        self.assertTrue(p.running)
        p.capital["day_ago"] = "20001"
        p.check_risk()
        self.assertFalse(p.running)
        self.assertFalse(any(o["ordType"] == "market" for o in sent(p)))
        p = fixture()
        p.timer_action(105000, local_hour=0, local_minute=0)
        self.assertNotIn("day_ago", p.capital)
        p.timer_action(110000, local_hour=12, local_minute=0)
        self.assertEqual(p.capital["day_ago"], D(10000))
        p.capital["total"] = "11000"
        p.timer_action(115000, local_hour=12, local_minute=0)
        self.assertEqual(p.capital["day_ago"], D(11000))

    def test_high_low_and_restart(self):
        p = fixture()
        p.markets["C0"]["ratioHL"] = "2.1"
        p.start_strategy("C0")
        self.assertEqual(sent(p), [])
        p.markets["C0"]["ratioHL"] = "2"
        p.check_single(p.rows[0])
        self.assertEqual(sent(p), [])
        p.compare_reply()
        self.assertEqual(len(sent(p)), 8)

    def test_reset(self):
        p = fixture()
        p.markets["C0"]["minSz"] = "2"
        p.start_strategy("C0")
        self.assertGreater(cast(D, p.rows[0]["歩差"]), D("0.01"))
        self.assertEqual(p.rows[0]["單數"], "1")
        count = len(p.events)
        p.reset_strategy("C0")
        self.assertEqual(len(p.events), count)
        p.now_ms += 3000
        p.reset_strategy("C0")
        self.assertEqual(len(p.events), count)

    def test_reply_manual_count(self):
        p = fixture(1)
        p.rows[0]["單數"] = "1"
        p.replies = {
            str(i): dict(instId="C0", clOrdId="manual", state="live") for i in range(2)
        }
        p.compare_reply()
        self.assertEqual(sent(p), [])

    def test_partial_full_duplicate(self):
        p = fixture()
        p.start_strategy("C0")
        original = sent(p)[0]
        p.events.clear()
        p.orders(report(original, "partially_filled"))
        self.assertEqual(sent(p), [])
        p.orders(report(original))
        self.assertEqual(len(sent(p)), 2)
        self.assertEqual(p.markets["C0"]["price"], D(99))
        self.assertEqual([r["hold"] for r in p.rows], ["0", "0"])
        # The original 100/1.01 buy limit is rounded down to 99.00 first.
        # Refill anchors to that order limit: 99/1.01^2 and 99*1.01.
        self.assertEqual([o["px"] for o in sent(p)], ["97.04", "99.99"])
        p.events.clear()
        p.orders(report(original))
        self.assertEqual(sent(p), [])

    def test_report_states_ids(self):
        for state in ["canceled", "mmp_canceled", "partially_filled", "unknown"]:
            p = fixture()
            p.start_strategy("C0")
            od = sent(p)[0]
            p.events.clear()
            p.orders(report(od, state))
            self.assertEqual(sent(p), [])
            self.assertEqual(
                "ex1" in p.replies, state not in ["canceled", "mmp_canceled"]
            )
        p = fixture()
        od = p.make_order("C0", "xxxx", "buy", "", "1")
        p.orders(report(od))
        self.assertTrue(any(e["kind"] == "unsupported_source_path" for e in p.events))
        p = fixture()
        od["clOrdId"] = "manual"
        p.orders(report(od))
        self.assertEqual(sent(p), [])

    def test_transport_ack_matrix(self):
        for online in [False, True]:
            for running in [False, True]:
                for ws in [False, True]:
                    p = fixture()
                    p.online = online
                    p.running = running
                    p.ws_open = ws
                    ods = [
                        p.make_order("C0", str(i), "buy", str(100 + i), "1")
                        for i in range(21)
                    ]
                    p.transport("send", ods)
                    self.assertEqual(len(sent(p)), 21 if online and running else 0)
                    if online:
                        self.assertEqual([len(e["orders"]) for e in p.events], [20, 1])
                        self.assertEqual(p.events[0]["route"], "WS" if ws else "REST")
        p = fixture()
        p.start_strategy("C0")
        od = sent(p)[0]
        ack = dict(clOrdId=od["clOrdId"], ordId="real", sCode="0")
        p.rest_ack("send", [ack])
        self.assertEqual(p.replies["real"]["state"], "sent")
        p.rest_ack("cancel", [ack])
        self.assertEqual(p.replies["real"]["state"], "canceling")
        p.rest_ack("cancel", [ack | dict(sCode="1", sMsg="failed")])
        self.assertEqual(p.replies["real"]["state"], "failed")

    def test_cancel_states(self):
        p = fixture()
        states = ["new", "sent", "live", "partially_filled", "canceling", "failed"]
        p.replies = {
            str(i): dict(instId="C0", clOrdId="c", state=s)
            for i, s in enumerate(states)
        }
        p.cancel_all("C0")
        self.assertEqual([o["ordId"] for o in sent(p, "cancel")], ["1", "2", "3"])
        self.assertEqual(set(p.replies), {"1", "2", "3"})
        p.events.clear()
        p.markets["C0"]["instIdCode"] = 0
        p.cancel_all("C0")
        self.assertEqual(sent(p, "cancel"), [])

    def test_pause_fills_continue(self):
        p = fixture()
        p.start_strategy("C0")
        od = sent(p)[0]
        p.events.clear()
        p.paused = True
        p.timer_action(105000)
        self.assertEqual(p.events, [])
        p.orders(report(od))
        self.assertEqual(len(sent(p)), 2)

    def test_snapshots_poll_order(self):
        p = fixture(fixed="50")
        p.timer_action(105000, earn="100", trading=dict(eq="10000", availEq="9000"))
        self.assertEqual(p.capital["total"], D(10050))
        before = dict(p.capital)
        p.timer_action(110000)
        self.assertEqual(p.capital, before)
        p = fixture(LeverageUp="0.1")
        p.timer_action(105000, open_orders=[])
        self.assertGreater(cast(D, p.rows[0]["leverage"]), D(4))
        q = fixture()
        q.compare_reply()
        self.assertEqual(
            [(o["px"], o["sz"]) for o in sent(p)], [(o["px"], o["sz"]) for o in sent(q)]
        )

    def test_positions_cross_fallback_overwrite(self):
        p = fixture()
        p.positions_response(
            [
                dict(instId="C0", mgnMode="cross", pos="2"),
                dict(instId="C0", mgnMode="cross", pos="-1"),
                dict(instId="I", mgnMode="isolated", pos="100"),
                dict(instId="E", mgnMode="cross", pos="-2", last="0", notionalUsd="50"),
            ]
        )
        self.assertEqual(p.capital["position"], D(-150))
        self.assertEqual(p.rows[0]["hold"], D(-100))


if __name__ == "__main__":
    unittest.main(verbosity=2)
