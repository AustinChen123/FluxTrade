"""6P closed fixture vectors independently reconstructed from typed byte contracts."""
from copy import deepcopy
from decimal import Decimal as D
from hashlib import sha256
from struct import pack
from typing import cast

import pytest

from src.core.backtest import synthetic_scenario_codec as c
from spider_acceptance_fixtures import ACCOUNT, group, snapshot

PROFILE = "SYNTHETIC_P1_LIQUIDATION_V1"
BEFORE = "6158a5ae1d44e8306c56e617550a4cc8d6ad333d7fff2cceb5cdc2f17e7bb892"
AFTER = "dee6233edc4b6aae76b2bcdf27459aa049a6b3aaabc04ff7d2e93618b4af39b6"
LIQUIDATION = "7fb9f5bec4fe944a93de3c08e44d149a42279d93ff24c37b7712c0a5037db810"
ORDER = "cf125b37f9df463dfe002fede0c5f8dab40b7848f549ba0232309cc11b98daeb"
INITIAL = ("d462ef662ec7afee486297f6763adffa20690a92e92290e88f96469398980e9d", ORDER,
           "4695ea99e32a159d3bc5d9dddd3b6071f5301617fdb256fab6356d93b48bdffe", "134b24a87d3490abca366cadb5e081d698562474c3285e7b4e1393300c4da94d")
FINAL = ("4fe4cb189a4d0af356f106da00680f149a13984cd7c7073a03357a222962f4da", ORDER,
         "d03759993b78ed62d75ab18ce9b691cb0eee02b374fafe4df1292704f1e21a31", "542cb313c7e688a6107f408aa24414041cd4321bd7e1c75d26b1d2d7e9277fb4")
VECTORS = [
    ("INITIAL", "TRADING", "5a37afb42c186e355c0061296dabc028928cb8a3b4d944443645a033dcfaf237", "6b1c05759909cb5877e22c32aadcd0ba1d42d691d72ffe1deeb73709faaf4c88"),
    ("INITIAL", "POSITIONS", "f8a06b4b9570bed66c004efba40f6ae726516409168a33260e0c370c5bd7c85f", "d7d01f06e20507198cc7a9d9453b50c19ed9a0353fc17b32783cdb1db634c64e"),
    ("INITIAL", "OPEN_ORDERS", "42fea6c1b4625f044a630f130e86dee199dea390771b0fc99aaa4d0859344261", "67fbb8688e7045ffa93c2699030a9dd9721dd5f62e6a8c52782faeaa85658286"),
    ("POST-S6-L-MARK-1501", "TRADING", "649bd73e7acbb95c602ef1e41264d8e90fb702badd6fcaa71ce7df658893b76b", "f998a97f36243ff940c98df9612a2affbcb895853b1fe555d16ce43e4e2ad0e5"),
    ("POST-S6-L-MARK-1501", "POSITIONS", "ba76bb883c006b68633ead56fdfae050facc43d7df802487131ad8d55606d25f", "738629c13d0fb407ae4676d367dbbd4a71d7331df7f19cd5fb0dd1e9658c65c3"),
    ("POST-S6-L-MARK-1501", "OPEN_ORDERS", "34455a3c3d7c8b7da76f930a0188ac9d7c89f69268f2f74bd346e14e8562787a", "17b3a684f5b59aee8f0637679d4c642106ef191aead4c9b917609c1793f815bd"),
    ("FINAL", "TRADING", "7c0c67f19ac3562f8e4a8a32df7d7c386fe3f3458207bd478fe74b8d628e5fe9", "0394d5ce083b06d54fc4071b504f58b401222670a054dc6384aa06ffa0fa324c"),
    ("FINAL", "POSITIONS", "559b66f9cf856778d96ad48566c0177e6df6d9c914e1dfae843d82c7fae1123e", "db2bba4562410f0e93500196f997b559506da4e440d9a01e88c31fe0e2f5b331"),
    ("FINAL", "OPEN_ORDERS", "fb87864aaedbc71e83c4950fdd98cf0d8b17b0ac407c7b07f7e79fcf6fe0bfa2", "c5edcc402c28c938cff279bdaaaa0b353e7032d0bda7b76106aaa95ae93d71dd"),
]


def request():
    return cast(c.Group, group("S6-L-MARK-1501", 1501, "CONTEXT_MARKS", dict(
        expected_before=BEFORE, expected_after=AFTER, rows=[
            dict(product_id="BTC-USDT-SWAP", valid_from=1501, valid_to=3000, mark=D("50100")),
            dict(product_id="ETH-USDT-SWAP", valid_from=0, valid_to=3000, mark=D("1900"))]), 20))


def inspection(codec, initial):
    value = codec.inspect_state()
    assert (value["profile_id"], value["account_key"], value["config_id"]) == (PROFILE, ACCOUNT, "scenario-v1")
    assert tuple(value[k] for k in ("positions_digest", "orders_digest", "reservations_digest", "owner_state_digest")) == (INITIAL if initial else FINAL)
    assert (value["account_version"], value["cash"], value["gross_realized"], value["total_fees"]) == ((0, D("3"), D("0"), D("0")) if initial else (2, D("-1.01602"), D("-1"), D("3.01602")))
    assert (value["valuation_context_id"], value["gate"], value["lifecycle"]) == (BEFORE if initial else AFTER, "RUNNING", "RISK_STABLE" if initial else "LIQUIDATED_INSOLVENT")
    return value


def check_snapshot(codec, stage, kind, request_hash, payload_hash):
    initial = stage == "INITIAL"
    at, version = (1500, 0) if initial else (1501, 2)
    name = f"S6-L-{stage}-{kind.replace('_', '-')}"
    def text(s):
        return pack(">q", len(s.encode())) + s.encode()

    def texts(*values):
        return b"".join(map(text, values))
    raw = texts("SCENARIO_SNAPSHOT_REQUEST_V1", "snapshot_request_v1", "okx-scenario", "test", "A") + b"\0"
    raw += texts(name, kind, "OWNER_CURRENT") + b"\0" + pack(">q", at) + b"\0"
    assert sha256(raw).hexdigest() == request_hash
    raw = texts("SCENARIO_SNAPSHOT_PAYLOAD_V1", "SNAPSHOT", name) + bytes.fromhex(request_hash) + text(kind)
    raw += b"\1" + pack(">q", version) + pack(">q", at) + b"\0"
    if kind == "TRADING":
        equity, available = ("3", "-47") if initial else ("-1.01602", "-1.01602")
        raw += texts("TRADING_SNAPSHOT", "SUCCESS", equity, available)
        payload = dict(outcome="SUCCESS", equity=D(equity), available_equity=D(available))
    else:
        nonempty = initial and kind == "POSITIONS"
        raw += texts("POSITION_SNAPSHOT" if kind == "POSITIONS" else "OPEN_ORDER_SNAPSHOT", "SUCCESS") + pack(">q", int(nonempty))
        payload = dict(outcome="SUCCESS", rows=[dict(product_id="BTC-USDT-SWAP", margin_mode="cross", position_contracts=D("-1"), last_price=D("50000"), notional_usd=D("500"))] if nonempty else [])
        if nonempty:
            raw += texts("BTC-USDT-SWAP", "cross", "-1") + b"\1" + text("50000") + b"\1" + text("500")
    assert sha256(raw).hexdigest() == payload_hash
    fact = codec.capture_snapshot(cast(c.SnapshotRequest, snapshot(name, kind, at)))
    assert (fact["request_digest"], fact["payload_digest"], fact["captured_account_version"], fact["snapshot_as_of"]) == (request_hash, payload_hash, version, at)
    assert fact["immutable_payload"] == payload


def test_closed_liquidation_literal_vectors_and_terminal_precedence():
    codec = c.ScenarioCodec(PROFILE, ACCOUNT)
    initial = inspection(codec, True)
    for vector in VECTORS[:3]:
        check_snapshot(codec, *vector)
    assert codec.inspect_state() == initial
    original = codec.apply_group(request())
    assert original["classification"] == "COMMITTED" and original["rejections"] == []
    assert (original["account_version_before"], original["account_version_after"], original["gate_after"], original["lifecycle_after"]) == (0, 2, "RUNNING", "LIQUIDATED_INSOLVENT")
    assert (original.get("group_digest"), original["result_digest"], original["owner_state_digest"]) == ("856b48e28b1e5bedb22d3f13daf62be641f51868919de89a038b371000de20f3", "b2ae5e8b6f92947b1938e4b78760d90c152884f19d5ce9203ab227412fe8acf1", FINAL[3])
    assert original["committed_references"] == [dict(namespace="SOURCE", fact_id="S6-L-MARK-1501"), dict(namespace="LIQUIDATION", fact_id=LIQUIDATION)]
    final = inspection(codec, False)
    for vector in VECTORS[3:]:
        check_snapshot(codec, *vector)
    assert c._encode(codec.apply_group(request())) == c._encode(original)
    fresh = deepcopy(request())
    fresh["group_id"] = fresh["members"][0]["stamp"]["event_id"] = "fresh"
    rejected = codec.apply_group(fresh)
    assert (rejected["classification"], rejected.get("failure"), rejected["gate_after"]) == ("REJECTED", "RUN_TERMINAL", "RUNNING")
    assert codec.inspect_state() == final
    malformed = deepcopy(request())
    malformed["members"][0]["stamp"]["effective_at"] = True
    with pytest.raises(ValueError, match="^INVALID_SCHEMA$"):
        codec.apply_group(malformed)
    assert codec.inspect_state() == final
    conflict = deepcopy(request())
    conflict["group_id"] = "conflict"
    conflict["members"][0]["stamp"]["scenario_ordinal"] = 0
    result = codec.apply_group(conflict)
    assert (result["classification"], result.get("failure"), result["gate_after"]) == ("FAULT", "EVENT_ID_CONFLICT", "FAILED")
    failed = codec.inspect_state()
    assert all(failed[k] == final[k] for k in ("cash", "gross_realized", "total_fees", "account_version", "positions_digest", "orders_digest", "reservations_digest"))
    assert c._encode(codec.apply_group(request())) == c._encode(original)
