from src.core.ws_connector import _sign_payload_binance


def test_sign_payload_binance_matches_known_hmac_sha256_vector() -> None:
    payload = "The quick brown fox jumps over the lazy dog"
    secret = "key"

    assert (
        _sign_payload_binance(payload, secret)
        == "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
    )


def test_sign_payload_binance_accepts_dict_payloads_in_insertion_order() -> None:
    payload = {
        "symbol": "LTCBTC",
        "side": "BUY",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": "1",
        "price": "0.1",
        "recvWindow": "5000",
        "timestamp": "1499827319559",
    }
    secret = "NhqPtmdSJYYUnHB3O3rQ5JYkqV8nG8fI"

    assert (
        _sign_payload_binance(payload, secret)
        == "d9d6f1eae0e75783037ddf14087af6307f4f0da8a9dc6da99a4e7778eccd536a"
    )


def test_sign_payload_binance_changes_when_payload_changes() -> None:
    secret = "NhqPtmdSJYYUnHB3O3rQ5JYkqV8nG8fI"

    assert _sign_payload_binance("timestamp=1", secret) != _sign_payload_binance(
        "timestamp=2",
        secret,
    )
