"""Context-independent exact Decimal arithmetic for shared core projections."""

from decimal import Decimal
from fractions import Fraction


def canonical_decimal_text(value: Decimal) -> str:
    """Render one finite exact Decimal as context-independent fixed-point text."""
    if type(value) is not Decimal or not value.is_finite():
        raise ValueError("value must be a finite exact Decimal")
    if value.is_zero():
        return "0"
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def exact_decimal_add(left: Decimal, right: Decimal) -> Decimal:
    """Add finite Decimals without consulting the ambient context."""
    left_sign, left_digits, left_exponent = left.as_tuple()
    right_sign, right_digits, right_exponent = right.as_tuple()
    assert isinstance(left_exponent, int) and isinstance(right_exponent, int)
    exponent = min(left_exponent, right_exponent)
    left_coefficient = int("".join(map(str, left_digits))) * 10 ** (
        left_exponent - exponent
    )
    right_coefficient = int("".join(map(str, right_digits))) * 10 ** (
        right_exponent - exponent
    )
    total = (-left_coefficient if left_sign else left_coefficient) + (
        -right_coefficient if right_sign else right_coefficient
    )
    if total == 0:
        return Decimal(0)
    return Decimal(
        (
            int(total < 0),
            tuple(map(int, str(abs(total)))),
            exponent,
        )
    )


def exact_decimal_subtract(left: Decimal, right: Decimal) -> Decimal:
    """Subtract finite Decimals without consulting the ambient context."""
    return exact_decimal_add(left, right.copy_negate())


def exact_decimal_subtract_preserving_zero_scale(
    left: Decimal,
    right: Decimal,
) -> Decimal:
    """Subtract exactly while matching Decimal subtraction's zero exponent."""
    result = exact_decimal_subtract(left, right)
    if result != 0:
        return result
    left_exponent = left.as_tuple().exponent
    right_exponent = right.as_tuple().exponent
    assert isinstance(left_exponent, int) and isinstance(right_exponent, int)
    return Decimal((0, (0,), min(left_exponent, right_exponent)))


def decimal_from_fraction(value: Fraction, *, places: int) -> Decimal:
    """Round an exact fraction to fixed places using context-free half-even."""
    if type(value) is not Fraction:
        raise TypeError("value must be an exact Fraction")
    if type(places) is not int or places < 0:
        raise ValueError("places must be a nonnegative exact integer")

    scaled = abs(value.numerator) * 10**places
    coefficient, remainder = divmod(scaled, value.denominator)
    twice_remainder = remainder * 2
    if twice_remainder > value.denominator or (
        twice_remainder == value.denominator and coefficient % 2 == 1
    ):
        coefficient += 1

    digits = tuple(map(int, str(coefficient)))
    return Decimal((int(value < 0), digits, -places))
