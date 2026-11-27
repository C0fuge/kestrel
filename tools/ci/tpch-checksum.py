"""Versioned TPC-H value checksums shared by native generation and Python Arrow clients."""

import datetime
import decimal
import hashlib


VERSION = "tpch-v2-sha256-64-numeric2"
CENT = decimal.Decimal("0.01")


def canonical_value(value):
    """Normalize TPC-H scalars with exact integers and HALF_EVEN two-decimal numeric comparison."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool:true" if value else "bool:false"
    if isinstance(value, (int, float, decimal.Decimal)):
        number = decimal.Decimal.from_float(value) if isinstance(value, float) else decimal.Decimal(value)
        if number.is_nan():
            return "num:nan"
        if number.is_infinite():
            return "num:-inf" if number.is_signed() else "num:+inf"
        # DuckDB HUGEINT/DECIMAL can exceed the default 28-digit Decimal context.
        with decimal.localcontext() as context:
            context.prec = max(34, len(number.as_tuple().digits) + 3, number.adjusted() + 3)
            rounded = number.quantize(CENT, rounding=decimal.ROUND_HALF_EVEN)
        return "num:0.00" if rounded.is_zero() else "num:" + format(rounded, "f")
    if isinstance(value, str):
        return "text:" + value
    if type(value) is datetime.date:
        return "text:" + value.isoformat()
    raise TypeError(f"Unsupported TPC-H checksum scalar: {type(value).__name__}")


def row_bytes(values):
    """Length-prefix UTF-8 tokens so separators, nulls and Unicode remain unambiguous."""
    result = bytearray()
    for value in values:
        token = canonical_value(value).encode("utf-8")
        result.extend(str(len(token)).encode("ascii") + b":" + token)
    return bytes(result)


def checksum_rows(rows):
    """Sum the first 64 SHA-256 bits of every row modulo 2^64, preserving duplicate rows."""
    return sum(int.from_bytes(hashlib.sha256(row_bytes(row)).digest()[:8], "big") for row in rows) % (1 << 64)
