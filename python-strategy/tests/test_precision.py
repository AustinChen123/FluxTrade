from decimal import Decimal

import pytest

from src.core.precision import PrecisionCodec, PrecisionSpec, RoundingMode


def test_precision_codec_round_trips_price_units():
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.10"),
            quantity_step=Decimal("0.001"),
        )
    )

    units = codec.encode_price(Decimal("104523.70"))

    assert units == 1_045_237
    assert codec.decode_price(units) == Decimal("104523.70")


def test_precision_codec_round_trips_quantity_units():
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.10"),
            quantity_step=Decimal("0.001"),
        )
    )

    units = codec.encode_quantity(Decimal("0.123"))

    assert units == 123
    assert codec.decode_quantity(units) == Decimal("0.123")


def test_precision_codec_defaults_quantity_rounding_down():
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.10"),
            quantity_step=Decimal("0.001"),
        )
    )

    units = codec.encode_quantity(Decimal("0.1239"))

    assert units == 123
    assert codec.decode_quantity(units) == Decimal("0.123")


def test_precision_codec_supports_explicit_rounding_modes():
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.10"),
            quantity_step=Decimal("0.001"),
        )
    )

    assert codec.encode_price(Decimal("104523.74"), rounding=RoundingMode.DOWN) == 1_045_237
    assert codec.encode_price(Decimal("104523.71"), rounding=RoundingMode.UP) == 1_045_238
    assert codec.encode_price(Decimal("104523.75"), rounding=RoundingMode.NEAREST) == 1_045_238


def test_precision_codec_encodes_fee_rates():
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.10"),
            quantity_step=Decimal("0.001"),
            fee_rate_step=Decimal("0.000001"),
        )
    )

    units = codec.encode_fee_rate(Decimal("0.0006"))

    assert units == 600
    assert codec.decode_fee_rate(units) == Decimal("0.000600")


def test_precision_codec_accepts_string_inputs_without_float_rounding():
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.01"),
            quantity_step=Decimal("0.0001"),
        )
    )

    assert codec.encode_price("0.29") == 29
    assert codec.decode_price(29) == Decimal("0.29")


def test_precision_codec_rejects_float_inputs():
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.01"),
            quantity_step=Decimal("0.0001"),
        )
    )

    with pytest.raises(TypeError, match="Decimal, str, or int"):
        codec.encode_price(0.29)


def test_precision_codec_rejects_negative_values():
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.01"),
            quantity_step=Decimal("0.0001"),
        )
    )

    with pytest.raises(ValueError, match="non-negative"):
        codec.encode_price(Decimal("-1"))


def test_precision_spec_rejects_non_positive_steps():
    with pytest.raises(ValueError, match="price_tick must be positive"):
        PrecisionSpec(
            price_tick=Decimal("0"),
            quantity_step=Decimal("0.001"),
        )


def test_precision_spec_requires_decimal_steps():
    with pytest.raises(TypeError, match="price_tick must be Decimal"):
        PrecisionSpec(
            price_tick="0.01",
            quantity_step=Decimal("0.001"),
        )
