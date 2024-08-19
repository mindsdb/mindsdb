# Copyright 2016-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tools for working with the BSON decimal128 type.

.. versionadded:: 3.4
"""
from __future__ import annotations

import decimal
import struct
from typing import Any, Sequence, Tuple, Type, Union

_PACK_64 = struct.Struct("<Q").pack
_UNPACK_64 = struct.Struct("<Q").unpack

_EXPONENT_MASK = 3 << 61
_EXPONENT_BIAS = 6176
_EXPONENT_MAX = 6144
_EXPONENT_MIN = -6143
_MAX_DIGITS = 34

_INF = 0x7800000000000000
_NAN = 0x7C00000000000000
_SNAN = 0x7E00000000000000
_SIGN = 0x8000000000000000

_NINF = (_INF + _SIGN, 0)
_PINF = (_INF, 0)
_NNAN = (_NAN + _SIGN, 0)
_PNAN = (_NAN, 0)
_NSNAN = (_SNAN + _SIGN, 0)
_PSNAN = (_SNAN, 0)

_CTX_OPTIONS = {
    "prec": _MAX_DIGITS,
    "rounding": decimal.ROUND_HALF_EVEN,
    "Emin": _EXPONENT_MIN,
    "Emax": _EXPONENT_MAX,
    "capitals": 1,
    "flags": [],
    "traps": [decimal.InvalidOperation, decimal.Overflow, decimal.Inexact],
    "clamp": 1,
}

_DEC128_CTX = decimal.Context(**_CTX_OPTIONS.copy())  # type: ignore
_VALUE_OPTIONS = Union[decimal.Decimal, float, str, Tuple[int, Sequence[int], int]]


def create_decimal128_context() -> decimal.Context:
    """Returns an instance of :class:`decimal.Context` appropriate
    for working with IEEE-754 128-bit decimal floating point values.
    """
    opts = _CTX_OPTIONS.copy()
    opts["traps"] = []
    return decimal.Context(**opts)  # type: ignore


def _decimal_to_128(value: _VALUE_OPTIONS) -> Tuple[int, int]:
    """Converts a decimal.Decimal to BID (high bits, low bits).

    :param value: An instance of decimal.Decimal
    """
    with decimal.localcontext(_DEC128_CTX) as ctx:
        value = ctx.create_decimal(value)

    if value.is_infinite():
        return _NINF if value.is_signed() else _PINF

    sign, digits, exponent = value.as_tuple()

    if value.is_nan():
        if digits:
            raise ValueError("NaN with debug payload is not supported")
        if value.is_snan():
            return _NSNAN if value.is_signed() else _PSNAN
        return _NNAN if value.is_signed() else _PNAN

    significand = int("".join([str(digit) for digit in digits]))
    bit_length = significand.bit_length()

    high = 0
    low = 0
    for i in range(min(64, bit_length)):
        if significand & (1 << i):
            low |= 1 << i

    for i in range(64, bit_length):
        if significand & (1 << i):
            high |= 1 << (i - 64)

    biased_exponent = exponent + _EXPONENT_BIAS  # type: ignore[operator]

    if high >> 49 == 1:
        high = high & 0x7FFFFFFFFFFF
        high |= _EXPONENT_MASK
        high |= (biased_exponent & 0x3FFF) << 47
    else:
        high |= biased_exponent << 49

    if sign:
        high |= _SIGN

    return high, low


class Decimal128:
    """BSON Decimal128 type::

      >>> Decimal128(Decimal("0.0005"))
      Decimal128('0.0005')
      >>> Decimal128("0.0005")
      Decimal128('0.0005')
      >>> Decimal128((3474527112516337664, 5))
      Decimal128('0.0005')

    :param value: An instance of :class:`decimal.Decimal`, string, or tuple of
        (high bits, low bits) from Binary Integer Decimal (BID) format.

    .. note:: :class:`~Decimal128` uses an instance of :class:`decimal.Context`
      configured for IEEE-754 Decimal128 when validating parameters.
      Signals like :class:`decimal.InvalidOperation`, :class:`decimal.Inexact`,
      and :class:`decimal.Overflow` are trapped and raised as exceptions::

        >>> Decimal128(".13.1")
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          ...
        decimal.InvalidOperation: [<class 'decimal.ConversionSyntax'>]
        >>>
        >>> Decimal128("1E-6177")
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          ...
        decimal.Inexact: [<class 'decimal.Inexact'>]
        >>>
        >>> Decimal128("1E6145")
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          ...
        decimal.Overflow: [<class 'decimal.Overflow'>, <class 'decimal.Rounded'>]

      To ensure the result of a calculation can always be stored as BSON
      Decimal128 use the context returned by
      :func:`create_decimal128_context`::

        >>> import decimal
        >>> decimal128_ctx = create_decimal128_context()
        >>> with decimal.localcontext(decimal128_ctx) as ctx:
        ...     Decimal128(ctx.create_decimal(".13.3"))
        ...
        Decimal128('NaN')
        >>>
        >>> with decimal.localcontext(decimal128_ctx) as ctx:
        ...     Decimal128(ctx.create_decimal("1E-6177"))
        ...
        Decimal128('0E-6176')
        >>>
        >>> with decimal.localcontext(DECIMAL128_CTX) as ctx:
        ...     Decimal128(ctx.create_decimal("1E6145"))
        ...
        Decimal128('Infinity')

      To match the behavior of MongoDB's Decimal128 implementation
      str(Decimal(value)) may not match str(Decimal128(value)) for NaN values::

        >>> Decimal128(Decimal('NaN'))
        Decimal128('NaN')
        >>> Decimal128(Decimal('-NaN'))
        Decimal128('NaN')
        >>> Decimal128(Decimal('sNaN'))
        Decimal128('NaN')
        >>> Decimal128(Decimal('-sNaN'))
        Decimal128('NaN')

      However, :meth:`~Decimal128.to_decimal` will return the exact value::

        >>> Decimal128(Decimal('NaN')).to_decimal()
        Decimal('NaN')
        >>> Decimal128(Decimal('-NaN')).to_decimal()
        Decimal('-NaN')
        >>> Decimal128(Decimal('sNaN')).to_decimal()
        Decimal('sNaN')
        >>> Decimal128(Decimal('-sNaN')).to_decimal()
        Decimal('-sNaN')

      Two instances of :class:`Decimal128` compare equal if their Binary
      Integer Decimal encodings are equal::

        >>> Decimal128('NaN') == Decimal128('NaN')
        True
        >>> Decimal128('NaN').bid == Decimal128('NaN').bid
        True

      This differs from :class:`decimal.Decimal` comparisons for NaN::

        >>> Decimal('NaN') == Decimal('NaN')
        False
    """

    __slots__ = ("__high", "__low")

    _type_marker = 19

    def __init__(self, value: _VALUE_OPTIONS) -> None:
        if isinstance(value, (str, decimal.Decimal)):
            self.__high, self.__low = _decimal_to_128(value)
        elif isinstance(value, (list, tuple)):
            if len(value) != 2:
                raise ValueError(
                    "Invalid size for creation of Decimal128 "
                    "from list or tuple. Must have exactly 2 "
                    "elements."
                )
            self.__high, self.__low = value  # type: ignore
        else:
            raise TypeError(f"Cannot convert {value!r} to Decimal128")

    def to_decimal(self) -> decimal.Decimal:
        """Returns an instance of :class:`decimal.Decimal` for this
        :class:`Decimal128`.
        """
        high = self.__high
        low = self.__low
        sign = 1 if (high & _SIGN) else 0

        if (high & _SNAN) == _SNAN:
            return decimal.Decimal((sign, (), "N"))  # type: ignore
        elif (high & _NAN) == _NAN:
            return decimal.Decimal((sign, (), "n"))  # type: ignore
        elif (high & _INF) == _INF:
            return decimal.Decimal((sign, (), "F"))  # type: ignore

        if (high & _EXPONENT_MASK) == _EXPONENT_MASK:
            exponent = ((high & 0x1FFFE00000000000) >> 47) - _EXPONENT_BIAS
            return decimal.Decimal((sign, (0,), exponent))
        else:
            exponent = ((high & 0x7FFF800000000000) >> 49) - _EXPONENT_BIAS

        arr = bytearray(15)
        mask = 0x00000000000000FF
        for i in range(14, 6, -1):
            arr[i] = (low & mask) >> ((14 - i) << 3)
            mask = mask << 8

        mask = 0x00000000000000FF
        for i in range(6, 0, -1):
            arr[i] = (high & mask) >> ((6 - i) << 3)
            mask = mask << 8

        mask = 0x0001000000000000
        arr[0] = (high & mask) >> 48

        # cdecimal only accepts a tuple for digits.
        digits = tuple(int(digit) for digit in str(int.from_bytes(arr, "big")))

        with decimal.localcontext(_DEC128_CTX) as ctx:
            return ctx.create_decimal((sign, digits, exponent))

    @classmethod
    def from_bid(cls: Type[Decimal128], value: bytes) -> Decimal128:
        """Create an instance of :class:`Decimal128` from Binary Integer
        Decimal string.

        :param value: 16 byte string (128-bit IEEE 754-2008 decimal floating
            point in Binary Integer Decimal (BID) format).
        """
        if not isinstance(value, bytes):
            raise TypeError("value must be an instance of bytes")
        if len(value) != 16:
            raise ValueError("value must be exactly 16 bytes")
        return cls((_UNPACK_64(value[8:])[0], _UNPACK_64(value[:8])[0]))  # type: ignore

    @property
    def bid(self) -> bytes:
        """The Binary Integer Decimal (BID) encoding of this instance."""
        return _PACK_64(self.__low) + _PACK_64(self.__high)

    def __str__(self) -> str:
        dec = self.to_decimal()
        if dec.is_nan():
            # Required by the drivers spec to match MongoDB behavior.
            return "NaN"
        return str(dec)

    def __repr__(self) -> str:
        return f"Decimal128('{self!s}')"

    def __setstate__(self, value: Tuple[int, int]) -> None:
        self.__high, self.__low = value

    def __getstate__(self) -> Tuple[int, int]:
        return self.__high, self.__low

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Decimal128):
            return self.bid == other.bid
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other
