# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.


class ISOFormatError(ValueError):
    """Raised when ISO 8601 string fails a format check."""


class RangeCheckError(ValueError):
    """Parent type of range check errors."""


class YearOutOfBoundsError(RangeCheckError):
    """Raised when year exceeds limits."""


class MonthOutOfBoundsError(RangeCheckError):
    """Raised when month is outside of 1..12."""


class WeekOutOfBoundsError(RangeCheckError):
    """Raised when week exceeds a year."""


class DayOutOfBoundsError(RangeCheckError):
    """Raised when day is outside of 1..365, 1..366 for leap year."""


class HoursOutOfBoundsError(RangeCheckError):
    """Raise when parsed hours are greater than 24."""


class MinutesOutOfBoundsError(RangeCheckError):
    """Raise when parsed seconds are greater than 60."""


class SecondsOutOfBoundsError(RangeCheckError):
    """Raise when parsed seconds are greater than 60."""


class MidnightBoundsError(RangeCheckError):
    """Raise when parsed time has an hour of 24 but is not midnight."""


class LeapSecondError(RangeCheckError):
    """Raised when attempting to parse a leap second"""
