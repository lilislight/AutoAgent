"""Exact USD storage units shared by policies and usage persistence."""

from decimal import Decimal, InvalidOperation, ROUND_CEILING, localcontext

USD_SCALE = 10**12
MAX_COST_UNITS = 2**63 - 1


def usd_units(value: str) -> int:
    """Validate a USD limit with at most twelve fractional decimal places."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("USD amounts must be decimal strings.")
    try:
        with localcontext() as context:
            context.prec = 60
            amount = Decimal(value)
            units = amount * USD_SCALE
            if not amount.is_finite() or amount < 0 or units != units.to_integral_value() or units > MAX_COST_UNITS:
                raise ValueError("USD amounts must be nonnegative, fit int64 storage, and have at most 12 decimal places.")
            return int(units)
    except InvalidOperation as error:
        raise ValueError("Invalid USD amount.") from error


def calculated_units(amount: Decimal) -> int:
    """Round a positive computed charge up to the nearest trillionth of a USD."""
    with localcontext() as context:
        context.prec = 60
        if not amount.is_finite() or amount < 0:
            raise ValueError("Calculated cost must be finite and nonnegative.")
        units = int((amount * USD_SCALE).to_integral_value(rounding=ROUND_CEILING))
        if units > MAX_COST_UNITS:
            raise ValueError("Calculated cost exceeds int64 storage.")
        return units


def units_usd(units: int) -> str:
    with localcontext() as context:
        context.prec = 60
        value = format(Decimal(units) / USD_SCALE, "f")
    return value.rstrip("0").rstrip(".") if "." in value else value
