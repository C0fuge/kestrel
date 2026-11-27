"""Fixed cross-language oracle vectors for the versioned TPC-H value checksum."""

import datetime
from decimal import Decimal, localcontext
from pathlib import Path
import runpy
import unittest


CHECKSUM = runpy.run_path(str(Path(__file__).with_name("tpch-checksum.py")))


class TpchChecksumTest(unittest.TestCase):
    """Validate expected oracle values rather than merely comparing two clients to each other."""

    def test_q12_native_hugeint_and_arrow_decimal_match_fixed_oracle(self):
        """SUM(CASE) Python ints and Arrow decimal128 values must share the expected SF1 checksum."""
        native = [("MAIL", 6202, 9324), ("SHIP", 6200, 9262)]
        arrow = [(name, Decimal(high), Decimal(low)) for name, high, low in native]
        for rows in (native, arrow):
            self.assertEqual(0x66191BF0FE853F3D, CHECKSUM["checksum_rows"](rows))

    def test_large_exact_values_and_half_even_binary_float_semantics(self):
        """Integers beyond double/context precision stay exact, with explicit decimal and float tie rules."""
        row = [2**127, -(2**127), 9007199254740993, Decimal("1.005"), Decimal("1.015"),
               1.005, 1.015, Decimal("123456789012345678901234567890.125")]
        with localcontext() as context:
            context.prec = 4
            self.assertEqual(0xCE8996EF5ECCD4B9, CHECKSUM["checksum_rows"]([row]))
            self.assertEqual("num:170141183460469231731687303715884105728.00",
                             CHECKSUM["canonical_value"](2**127))
            self.assertEqual(CHECKSUM["canonical_value"](2**127),
                             CHECKSUM["canonical_value"](Decimal(str(2**127))))
        self.assertNotEqual(CHECKSUM["checksum_rows"]([(9007199254740993,)]),
                            CHECKSUM["checksum_rows"]([(9007199254740992,)]))
        self.assertEqual("num:1.02", CHECKSUM["canonical_value"](Decimal("1.015")))
        self.assertEqual("num:1.01", CHECKSUM["canonical_value"](1.015))

    def test_special_numbers_signed_zero_null_and_boolean_fixed_vector(self):
        """NaNs, infinities and rounded signed zero have deterministic, type-tagged representations."""
        row = [None, "None", True, False, 0, -0.0, Decimal("-0.00"), float("nan"), float("inf"), float("-inf")]
        self.assertEqual(0x93494177E701E149, CHECKSUM["checksum_rows"]([row]))
        for value in (float("nan"), Decimal("NaN"), Decimal("-sNaN42")):
            self.assertEqual("num:nan", CHECKSUM["canonical_value"](value))
        for value in (0, -0.0, Decimal("-0.004"), Decimal("-0.005")):
            self.assertEqual("num:0.00", CHECKSUM["canonical_value"](value))
        self.assertEqual("num:-0.01", CHECKSUM["canonical_value"](Decimal("-0.006")))

    def test_utf8_framing_date_and_separators_have_fixed_vectors(self):
        """UTF-8 byte lengths prevent delimiter collisions across null/string and Unicode cells."""
        row = ["a|b", "a:b", "é", "🙂", datetime.date(1994, 1, 1), "", None]
        self.assertEqual(0xA2DBAFA4BAF6CA42, CHECKSUM["checksum_rows"]([row]))
        frame = [1, "a|b", None, "é"]
        self.assertEqual(bytes.fromhex("383a6e756d3a312e3030383a746578743a617c62343a6e756c6c373a746578743ac3a9"),
                         CHECKSUM["row_bytes"](frame))
        self.assertEqual(0xC7055CA8781D8861, CHECKSUM["checksum_rows"]([frame]))
        self.assertNotEqual(CHECKSUM["checksum_rows"]([("a|b", "c")]),
                            CHECKSUM["checksum_rows"]([("a", "b|c")]))
        self.assertEqual(0x70F6A9588EA18F67,
                         CHECKSUM["checksum_rows"]([[None, "null", "<null>", 1, "1.00"]]))
        self.assertNotEqual(CHECKSUM["row_bytes"]([None]), CHECKSUM["row_bytes"](["null"]))
        self.assertNotEqual(CHECKSUM["row_bytes"]([None]), CHECKSUM["row_bytes"](["<null>"]))
        self.assertNotEqual(CHECKSUM["row_bytes"]([1]), CHECKSUM["row_bytes"](["1.00"]))

    def test_answer_order_and_duplicates_are_explicit(self):
        """Answer order is ignored, duplicate rows contribute again, and an empty answer is zero."""
        rows = [("MAIL", 6202, 9324), ("SHIP", 6200, 9262)]
        expected = 0x66191BF0FE853F3D
        self.assertEqual(expected, CHECKSUM["checksum_rows"](reversed(rows)))
        self.assertEqual((2 * expected) % (1 << 64), CHECKSUM["checksum_rows"](rows + rows))
        self.assertEqual(0, CHECKSUM["checksum_rows"]([]))
        with self.assertRaises(TypeError):
            CHECKSUM["checksum_rows"]([(datetime.datetime(1994, 1, 1),)])
        with self.assertRaises(UnicodeEncodeError):
            CHECKSUM["checksum_rows"]([("\ud800",)])


if __name__ == "__main__":
    unittest.main()
