# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

import datetime
import unittest

from aniso8601 import compat
from aniso8601.builders import (
    DatetimeTuple,
    DateTuple,
    DurationTuple,
    IntervalTuple,
    Limit,
    TimeTuple,
    TimezoneTuple,
)
from aniso8601.builders.python import (
    FractionalComponent,
    PythonTimeBuilder,
    _cast_to_fractional_component,
    fractional_range_check,
    year_range_check,
)
from aniso8601.exceptions import (
    DayOutOfBoundsError,
    HoursOutOfBoundsError,
    ISOFormatError,
    LeapSecondError,
    MidnightBoundsError,
    MinutesOutOfBoundsError,
    MonthOutOfBoundsError,
    SecondsOutOfBoundsError,
    WeekOutOfBoundsError,
    YearOutOfBoundsError,
)
from aniso8601.utcoffset import UTCOffset


class TestPythonTimeBuilder_UtiltyFunctions(unittest.TestCase):
    def test_year_range_check(self):
        yearlimit = Limit(
            "Invalid year string.",
            0000,
            9999,
            YearOutOfBoundsError,
            "Year must be between 1..9999.",
            None,
        )

        self.assertEqual(year_range_check("1", yearlimit), 1000)

    def test_fractional_range_check(self):
        limit = Limit(
            "Invalid string.", -1, 1, ValueError, "Value must be between -1..1.", None
        )

        self.assertEqual(fractional_range_check(10, "1", limit), 1)
        self.assertEqual(fractional_range_check(10, "-1", limit), -1)
        self.assertEqual(
            fractional_range_check(10, "0.1", limit), FractionalComponent(0, 1)
        )
        self.assertEqual(
            fractional_range_check(10, "-0.1", limit), FractionalComponent(-0, 1)
        )

        with self.assertRaises(ValueError):
            fractional_range_check(10, "1.1", limit)

        with self.assertRaises(ValueError):
            fractional_range_check(10, "-1.1", limit)

    def test_cast_to_fractional_component(self):
        self.assertEqual(
            _cast_to_fractional_component(10, "1.1"), FractionalComponent(1, 1)
        )
        self.assertEqual(
            _cast_to_fractional_component(10, "-1.1"), FractionalComponent(-1, 1)
        )

        self.assertEqual(
            _cast_to_fractional_component(100, "1.1"), FractionalComponent(1, 10)
        )
        self.assertEqual(
            _cast_to_fractional_component(100, "-1.1"), FractionalComponent(-1, 10)
        )


class TestPythonTimeBuilder(unittest.TestCase):
    def test_build_date(self):
        testtuples = (
            (
                {
                    "YYYY": "2013",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
                datetime.date(2013, 1, 1),
            ),
            (
                {
                    "YYYY": "0001",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
                datetime.date(1, 1, 1),
            ),
            (
                {
                    "YYYY": "1900",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
                datetime.date(1900, 1, 1),
            ),
            (
                {
                    "YYYY": "1981",
                    "MM": "04",
                    "DD": "05",
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
                datetime.date(1981, 4, 5),
            ),
            (
                {
                    "YYYY": "1981",
                    "MM": "04",
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
                datetime.date(1981, 4, 1),
            ),
            (
                {
                    "YYYY": "1981",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": "095",
                },
                datetime.date(1981, 4, 5),
            ),
            (
                {
                    "YYYY": "1981",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": "365",
                },
                datetime.date(1981, 12, 31),
            ),
            (
                {
                    "YYYY": "1980",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": "366",
                },
                datetime.date(1980, 12, 31),
            ),
            # Make sure we shift in zeros
            (
                {
                    "YYYY": "1",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
                datetime.date(1000, 1, 1),
            ),
            (
                {
                    "YYYY": "12",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
                datetime.date(1200, 1, 1),
            ),
            (
                {
                    "YYYY": "123",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
                datetime.date(1230, 1, 1),
            ),
        )

        for testtuple in testtuples:
            result = PythonTimeBuilder.build_date(**testtuple[0])
            self.assertEqual(result, testtuple[1])

        # Test weekday
        testtuples = (
            (
                {
                    "YYYY": "2004",
                    "MM": None,
                    "DD": None,
                    "Www": "53",
                    "D": None,
                    "DDD": None,
                },
                datetime.date(2004, 12, 27),
                0,
            ),
            (
                {
                    "YYYY": "2009",
                    "MM": None,
                    "DD": None,
                    "Www": "01",
                    "D": None,
                    "DDD": None,
                },
                datetime.date(2008, 12, 29),
                0,
            ),
            (
                {
                    "YYYY": "2010",
                    "MM": None,
                    "DD": None,
                    "Www": "01",
                    "D": None,
                    "DDD": None,
                },
                datetime.date(2010, 1, 4),
                0,
            ),
            (
                {
                    "YYYY": "2009",
                    "MM": None,
                    "DD": None,
                    "Www": "53",
                    "D": None,
                    "DDD": None,
                },
                datetime.date(2009, 12, 28),
                0,
            ),
            (
                {
                    "YYYY": "2009",
                    "MM": None,
                    "DD": None,
                    "Www": "01",
                    "D": "1",
                    "DDD": None,
                },
                datetime.date(2008, 12, 29),
                0,
            ),
            (
                {
                    "YYYY": "2009",
                    "MM": None,
                    "DD": None,
                    "Www": "53",
                    "D": "7",
                    "DDD": None,
                },
                datetime.date(2010, 1, 3),
                6,
            ),
            (
                {
                    "YYYY": "2010",
                    "MM": None,
                    "DD": None,
                    "Www": "01",
                    "D": "1",
                    "DDD": None,
                },
                datetime.date(2010, 1, 4),
                0,
            ),
            (
                {
                    "YYYY": "2004",
                    "MM": None,
                    "DD": None,
                    "Www": "53",
                    "D": "6",
                    "DDD": None,
                },
                datetime.date(2005, 1, 1),
                5,
            ),
        )

        for testtuple in testtuples:
            result = PythonTimeBuilder.build_date(**testtuple[0])
            self.assertEqual(result, testtuple[1])
            self.assertEqual(result.weekday(), testtuple[2])

    def test_build_time(self):
        testtuples = (
            ({}, datetime.time()),
            ({"hh": "12.5"}, datetime.time(hour=12, minute=30)),
            (
                {"hh": "23.99999999997"},
                datetime.time(hour=23, minute=59, second=59, microsecond=999999),
            ),
            ({"hh": "1", "mm": "23"}, datetime.time(hour=1, minute=23)),
            (
                {"hh": "1", "mm": "23.4567"},
                datetime.time(hour=1, minute=23, second=27, microsecond=402000),
            ),
            (
                {"hh": "14", "mm": "43.999999997"},
                datetime.time(hour=14, minute=43, second=59, microsecond=999999),
            ),
            (
                {"hh": "1", "mm": "23", "ss": "45"},
                datetime.time(hour=1, minute=23, second=45),
            ),
            (
                {"hh": "23", "mm": "21", "ss": "28.512400"},
                datetime.time(hour=23, minute=21, second=28, microsecond=512400),
            ),
            (
                {"hh": "01", "mm": "03", "ss": "11.858714"},
                datetime.time(hour=1, minute=3, second=11, microsecond=858714),
            ),
            (
                {"hh": "14", "mm": "43", "ss": "59.9999997"},
                datetime.time(hour=14, minute=43, second=59, microsecond=999999),
            ),
            ({"hh": "24"}, datetime.time(hour=0)),
            ({"hh": "24", "mm": "00"}, datetime.time(hour=0)),
            ({"hh": "24", "mm": "00", "ss": "00"}, datetime.time(hour=0)),
            (
                {"tz": TimezoneTuple(False, None, "00", "00", "UTC")},
                datetime.time(tzinfo=UTCOffset(name="UTC", minutes=0)),
            ),
            (
                {
                    "hh": "23",
                    "mm": "21",
                    "ss": "28.512400",
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
                datetime.time(
                    hour=23,
                    minute=21,
                    second=28,
                    microsecond=512400,
                    tzinfo=UTCOffset(name="+00:00", minutes=0),
                ),
            ),
            (
                {
                    "hh": "1",
                    "mm": "23",
                    "tz": TimezoneTuple(False, None, "01", "00", "+1"),
                },
                datetime.time(
                    hour=1, minute=23, tzinfo=UTCOffset(name="+1", minutes=60)
                ),
            ),
            (
                {
                    "hh": "1",
                    "mm": "23.4567",
                    "tz": TimezoneTuple(True, None, "01", "00", "-1"),
                },
                datetime.time(
                    hour=1,
                    minute=23,
                    second=27,
                    microsecond=402000,
                    tzinfo=UTCOffset(name="-1", minutes=-60),
                ),
            ),
            (
                {
                    "hh": "23",
                    "mm": "21",
                    "ss": "28.512400",
                    "tz": TimezoneTuple(False, None, "01", "30", "+1:30"),
                },
                datetime.time(
                    hour=23,
                    minute=21,
                    second=28,
                    microsecond=512400,
                    tzinfo=UTCOffset(name="+1:30", minutes=90),
                ),
            ),
            (
                {
                    "hh": "23",
                    "mm": "21",
                    "ss": "28.512400",
                    "tz": TimezoneTuple(False, None, "11", "15", "+11:15"),
                },
                datetime.time(
                    hour=23,
                    minute=21,
                    second=28,
                    microsecond=512400,
                    tzinfo=UTCOffset(name="+11:15", minutes=675),
                ),
            ),
            (
                {
                    "hh": "23",
                    "mm": "21",
                    "ss": "28.512400",
                    "tz": TimezoneTuple(False, None, "12", "34", "+12:34"),
                },
                datetime.time(
                    hour=23,
                    minute=21,
                    second=28,
                    microsecond=512400,
                    tzinfo=UTCOffset(name="+12:34", minutes=754),
                ),
            ),
            (
                {
                    "hh": "23",
                    "mm": "21",
                    "ss": "28.512400",
                    "tz": TimezoneTuple(False, None, "00", "00", "UTC"),
                },
                datetime.time(
                    hour=23,
                    minute=21,
                    second=28,
                    microsecond=512400,
                    tzinfo=UTCOffset(name="UTC", minutes=0),
                ),
            ),
            # Make sure we truncate, not round
            # https://bitbucket.org/nielsenb/aniso8601/issues/10/sub-microsecond-precision-in-durations-is
            # https://bitbucket.org/nielsenb/aniso8601/issues/21/sub-microsecond-precision-is-lost-when
            (
                {"hh": "14.9999999999999999"},
                datetime.time(hour=14, minute=59, second=59, microsecond=999999),
            ),
            ({"mm": "0.00000000999"}, datetime.time()),
            ({"mm": "0.0000000999"}, datetime.time(microsecond=5)),
            ({"ss": "0.0000001"}, datetime.time()),
            ({"ss": "2.0000048"}, datetime.time(second=2, microsecond=4)),
        )

        for testtuple in testtuples:
            result = PythonTimeBuilder.build_time(**testtuple[0])
            self.assertEqual(result, testtuple[1])

    def test_build_datetime(self):
        testtuples = (
            (
                (
                    DateTuple("2019", "06", "05", None, None, None),
                    TimeTuple("01", "03", "11.858714", None),
                ),
                datetime.datetime(
                    2019, 6, 5, hour=1, minute=3, second=11, microsecond=858714
                ),
            ),
            (
                (
                    DateTuple("1234", "02", "03", None, None, None),
                    TimeTuple("23", "21", "28.512400", None),
                ),
                datetime.datetime(
                    1234, 2, 3, hour=23, minute=21, second=28, microsecond=512400
                ),
            ),
            (
                (
                    DateTuple("1981", "04", "05", None, None, None),
                    TimeTuple(
                        "23",
                        "21",
                        "28.512400",
                        TimezoneTuple(False, None, "11", "15", "+11:15"),
                    ),
                ),
                datetime.datetime(
                    1981,
                    4,
                    5,
                    hour=23,
                    minute=21,
                    second=28,
                    microsecond=512400,
                    tzinfo=UTCOffset(name="+11:15", minutes=675),
                ),
            ),
        )

        for testtuple in testtuples:
            result = PythonTimeBuilder.build_datetime(*testtuple[0])
            self.assertEqual(result, testtuple[1])

    def test_build_duration(self):
        testtuples = (
            (
                {
                    "PnY": "1",
                    "PnM": "2",
                    "PnD": "3",
                    "TnH": "4",
                    "TnM": "54",
                    "TnS": "6",
                },
                datetime.timedelta(days=428, hours=4, minutes=54, seconds=6),
            ),
            (
                {
                    "PnY": "1",
                    "PnM": "2",
                    "PnD": "3",
                    "TnH": "4",
                    "TnM": "54",
                    "TnS": "6.5",
                },
                datetime.timedelta(days=428, hours=4, minutes=54, seconds=6.5),
            ),
            ({"PnY": "1", "PnM": "2", "PnD": "3"}, datetime.timedelta(days=428)),
            ({"PnY": "1", "PnM": "2", "PnD": "3.5"}, datetime.timedelta(days=428.5)),
            (
                {"TnH": "4", "TnM": "54", "TnS": "6.5"},
                datetime.timedelta(hours=4, minutes=54, seconds=6.5),
            ),
            (
                {"TnH": "1", "TnM": "3", "TnS": "11.858714"},
                datetime.timedelta(hours=1, minutes=3, seconds=11, microseconds=858714),
            ),
            (
                {"TnH": "4", "TnM": "54", "TnS": "28.512400"},
                datetime.timedelta(
                    hours=4, minutes=54, seconds=28, microseconds=512400
                ),
            ),
            # Make sure we truncate, not round
            # https://bitbucket.org/nielsenb/aniso8601/issues/10/sub-microsecond-precision-in-durations-is
            # https://bitbucket.org/nielsenb/aniso8601/issues/21/sub-microsecond-precision-is-lost-when
            (
                {"PnY": "1999.9999999999999999"},
                datetime.timedelta(days=729999, seconds=86399, microseconds=999999),
            ),
            (
                {"PnM": "1.9999999999999999"},
                datetime.timedelta(
                    days=59, hours=23, minutes=59, seconds=59, microseconds=999999
                ),
            ),
            (
                {"PnW": "1.9999999999999999"},
                datetime.timedelta(
                    days=13, hours=23, minutes=59, seconds=59, microseconds=999999
                ),
            ),
            (
                {"PnD": "1.9999999999999999"},
                datetime.timedelta(
                    days=1, hours=23, minutes=59, seconds=59, microseconds=999999
                ),
            ),
            (
                {"TnH": "14.9999999999999999"},
                datetime.timedelta(
                    hours=14, minutes=59, seconds=59, microseconds=999999
                ),
            ),
            ({"TnM": "0.00000000999"}, datetime.timedelta(0)),
            ({"TnM": "0.0000000999"}, datetime.timedelta(microseconds=5)),
            ({"TnS": "0.0000001"}, datetime.timedelta(0)),
            ({"TnS": "2.0000048"}, datetime.timedelta(seconds=2, microseconds=4)),
            ({"PnY": "1"}, datetime.timedelta(days=365)),
            ({"PnY": "1.5"}, datetime.timedelta(days=547.5)),
            ({"PnM": "1"}, datetime.timedelta(days=30)),
            ({"PnM": "1.5"}, datetime.timedelta(days=45)),
            ({"PnW": "1"}, datetime.timedelta(days=7)),
            ({"PnW": "1.5"}, datetime.timedelta(days=10.5)),
            ({"PnD": "1"}, datetime.timedelta(days=1)),
            ({"PnD": "1.5"}, datetime.timedelta(days=1.5)),
            (
                {
                    "PnY": "0003",
                    "PnM": "06",
                    "PnD": "04",
                    "TnH": "12",
                    "TnM": "30",
                    "TnS": "05",
                },
                datetime.timedelta(days=1279, hours=12, minutes=30, seconds=5),
            ),
            (
                {
                    "PnY": "0003",
                    "PnM": "06",
                    "PnD": "04",
                    "TnH": "12",
                    "TnM": "30",
                    "TnS": "05.5",
                },
                datetime.timedelta(days=1279, hours=12, minutes=30, seconds=5.5),
            ),
            # Test timedelta limit
            (
                {"PnD": "999999999", "TnH": "23", "TnM": "59", "TnS": "59.999999"},
                datetime.timedelta.max,
            ),
            # Make sure we truncate, not round
            # https://bitbucket.org/nielsenb/aniso8601/issues/10/sub-microsecond-precision-in-durations-is
            (
                {
                    "PnY": "0001",
                    "PnM": "02",
                    "PnD": "03",
                    "TnH": "14",
                    "TnM": "43",
                    "TnS": "59.9999997",
                },
                datetime.timedelta(
                    days=428, hours=14, minutes=43, seconds=59, microseconds=999999
                ),
            ),
            # Verify overflows
            ({"TnH": "36"}, datetime.timedelta(days=1, hours=12)),
        )

        for testtuple in testtuples:
            result = PythonTimeBuilder.build_duration(**testtuple[0])
            self.assertEqual(result, testtuple[1])

    def test_build_interval(self):
        testtuples = (
            (
                {
                    "end": DatetimeTuple(
                        DateTuple("1981", "04", "05", None, None, None),
                        TimeTuple("01", "01", "00", None),
                    ),
                    "duration": DurationTuple(None, "1", None, None, None, None, None),
                },
                datetime.datetime(year=1981, month=4, day=5, hour=1, minute=1),
                datetime.datetime(year=1981, month=3, day=6, hour=1, minute=1),
            ),
            (
                {
                    "end": DateTuple("1981", "04", "05", None, None, None),
                    "duration": DurationTuple(None, "1", None, None, None, None, None),
                },
                datetime.date(year=1981, month=4, day=5),
                datetime.date(year=1981, month=3, day=6),
            ),
            (
                {
                    "end": DateTuple("2018", "03", "06", None, None, None),
                    "duration": DurationTuple(
                        "1.5", None, None, None, None, None, None
                    ),
                },
                datetime.date(year=2018, month=3, day=6),
                datetime.datetime(year=2016, month=9, day=4, hour=12),
            ),
            (
                {
                    "end": DateTuple("2014", "11", "12", None, None, None),
                    "duration": DurationTuple(None, None, None, None, "1", None, None),
                },
                datetime.date(year=2014, month=11, day=12),
                datetime.datetime(year=2014, month=11, day=11, hour=23),
            ),
            (
                {
                    "end": DateTuple("2014", "11", "12", None, None, None),
                    "duration": DurationTuple(None, None, None, None, "4", "54", "6.5"),
                },
                datetime.date(year=2014, month=11, day=12),
                datetime.datetime(
                    year=2014,
                    month=11,
                    day=11,
                    hour=19,
                    minute=5,
                    second=53,
                    microsecond=500000,
                ),
            ),
            (
                {
                    "end": DatetimeTuple(
                        DateTuple("2050", "03", "01", None, None, None),
                        TimeTuple(
                            "13",
                            "00",
                            "00",
                            TimezoneTuple(False, True, None, None, "Z"),
                        ),
                    ),
                    "duration": DurationTuple(None, None, None, None, "10", None, None),
                },
                datetime.datetime(
                    year=2050,
                    month=3,
                    day=1,
                    hour=13,
                    tzinfo=UTCOffset(name="UTC", minutes=0),
                ),
                datetime.datetime(
                    year=2050,
                    month=3,
                    day=1,
                    hour=3,
                    tzinfo=UTCOffset(name="UTC", minutes=0),
                ),
            ),
            # Make sure we truncate, not round
            # https://bitbucket.org/nielsenb/aniso8601/issues/10/sub-microsecond-precision-in-durations-is
            # https://bitbucket.org/nielsenb/aniso8601/issues/21/sub-microsecond-precision-is-lost-when
            (
                {
                    "end": DateTuple("2000", "01", "01", None, None, None),
                    "duration": DurationTuple(
                        "1999.9999999999999999", None, None, None, None, None, None
                    ),
                },
                datetime.date(year=2000, month=1, day=1),
                datetime.datetime(
                    year=1, month=4, day=30, hour=0, minute=0, second=0, microsecond=1
                ),
            ),
            (
                {
                    "end": DateTuple("1989", "03", "01", None, None, None),
                    "duration": DurationTuple(
                        None, "1.9999999999999999", None, None, None, None, None
                    ),
                },
                datetime.date(year=1989, month=3, day=1),
                datetime.datetime(
                    year=1988,
                    month=12,
                    day=31,
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=1,
                ),
            ),
            (
                {
                    "end": DateTuple("1989", "03", "01", None, None, None),
                    "duration": DurationTuple(
                        None, None, "1.9999999999999999", None, None, None, None
                    ),
                },
                datetime.date(year=1989, month=3, day=1),
                datetime.datetime(
                    year=1989,
                    month=2,
                    day=15,
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=1,
                ),
            ),
            (
                {
                    "end": DateTuple("1989", "03", "01", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, "1.9999999999999999", None, None, None
                    ),
                },
                datetime.date(year=1989, month=3, day=1),
                datetime.datetime(
                    year=1989,
                    month=2,
                    day=27,
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=1,
                ),
            ),
            (
                {
                    "end": DateTuple("2001", "01", "01", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, None, "14.9999999999999999", None, None
                    ),
                },
                datetime.date(year=2001, month=1, day=1),
                datetime.datetime(
                    year=2000,
                    month=12,
                    day=31,
                    hour=9,
                    minute=0,
                    second=0,
                    microsecond=1,
                ),
            ),
            (
                {
                    "end": DateTuple("2001", "01", "01", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, None, None, "0.00000000999", None
                    ),
                },
                datetime.date(year=2001, month=1, day=1),
                datetime.datetime(year=2001, month=1, day=1),
            ),
            (
                {
                    "end": DateTuple("2001", "01", "01", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, None, None, "0.0000000999", None
                    ),
                },
                datetime.date(year=2001, month=1, day=1),
                datetime.datetime(
                    year=2000,
                    month=12,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999995,
                ),
            ),
            (
                {
                    "end": DateTuple("2018", "03", "06", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, None, None, None, "0.0000001"
                    ),
                },
                datetime.date(year=2018, month=3, day=6),
                datetime.datetime(year=2018, month=3, day=6),
            ),
            (
                {
                    "end": DateTuple("2018", "03", "06", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, None, None, None, "2.0000048"
                    ),
                },
                datetime.date(year=2018, month=3, day=6),
                datetime.datetime(
                    year=2018,
                    month=3,
                    day=5,
                    hour=23,
                    minute=59,
                    second=57,
                    microsecond=999996,
                ),
            ),
            (
                {
                    "start": DatetimeTuple(
                        DateTuple("1981", "04", "05", None, None, None),
                        TimeTuple("01", "01", "00", None),
                    ),
                    "duration": DurationTuple(None, "1", None, "1", None, "1", None),
                },
                datetime.datetime(year=1981, month=4, day=5, hour=1, minute=1),
                datetime.datetime(year=1981, month=5, day=6, hour=1, minute=2),
            ),
            (
                {
                    "start": DateTuple("1981", "04", "05", None, None, None),
                    "duration": DurationTuple(None, "1", None, "1", None, None, None),
                },
                datetime.date(year=1981, month=4, day=5),
                datetime.date(year=1981, month=5, day=6),
            ),
            (
                {
                    "start": DateTuple("2018", "03", "06", None, None, None),
                    "duration": DurationTuple(
                        None, "2.5", None, None, None, None, None
                    ),
                },
                datetime.date(year=2018, month=3, day=6),
                datetime.date(year=2018, month=5, day=20),
            ),
            (
                {
                    "start": DateTuple("2014", "11", "12", None, None, None),
                    "duration": DurationTuple(None, None, None, None, "1", None, None),
                },
                datetime.date(year=2014, month=11, day=12),
                datetime.datetime(year=2014, month=11, day=12, hour=1, minute=0),
            ),
            (
                {
                    "start": DateTuple("2014", "11", "12", None, None, None),
                    "duration": DurationTuple(None, None, None, None, "4", "54", "6.5"),
                },
                datetime.date(year=2014, month=11, day=12),
                datetime.datetime(
                    year=2014,
                    month=11,
                    day=12,
                    hour=4,
                    minute=54,
                    second=6,
                    microsecond=500000,
                ),
            ),
            (
                {
                    "start": DatetimeTuple(
                        DateTuple("2050", "03", "01", None, None, None),
                        TimeTuple(
                            "13",
                            "00",
                            "00",
                            TimezoneTuple(False, True, None, None, "Z"),
                        ),
                    ),
                    "duration": DurationTuple(None, None, None, None, "10", None, None),
                },
                datetime.datetime(
                    year=2050,
                    month=3,
                    day=1,
                    hour=13,
                    tzinfo=UTCOffset(name="UTC", minutes=0),
                ),
                datetime.datetime(
                    year=2050,
                    month=3,
                    day=1,
                    hour=23,
                    tzinfo=UTCOffset(name="UTC", minutes=0),
                ),
            ),
            # Make sure we truncate, not round
            # https://bitbucket.org/nielsenb/aniso8601/issues/10/sub-microsecond-precision-in-durations-is
            (
                {
                    "start": DateTuple("0001", "01", "01", None, None, None),
                    "duration": DurationTuple(
                        "1999.9999999999999999", None, None, None, None, None, None
                    ),
                },
                datetime.date(year=1, month=1, day=1),
                datetime.datetime(
                    year=1999,
                    month=9,
                    day=3,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                ),
            ),
            (
                {
                    "start": DateTuple("1989", "03", "01", None, None, None),
                    "duration": DurationTuple(
                        None, "1.9999999999999999", None, None, None, None, None
                    ),
                },
                datetime.date(year=1989, month=3, day=1),
                datetime.datetime(
                    year=1989,
                    month=4,
                    day=29,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                ),
            ),
            (
                {
                    "start": DateTuple("1989", "03", "01", None, None, None),
                    "duration": DurationTuple(
                        None, None, "1.9999999999999999", None, None, None, None
                    ),
                },
                datetime.date(year=1989, month=3, day=1),
                datetime.datetime(
                    year=1989,
                    month=3,
                    day=14,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                ),
            ),
            (
                {
                    "start": DateTuple("1989", "03", "01", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, "1.9999999999999999", None, None, None
                    ),
                },
                datetime.date(year=1989, month=3, day=1),
                datetime.datetime(
                    year=1989,
                    month=3,
                    day=2,
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                ),
            ),
            (
                {
                    "start": DateTuple("2001", "01", "01", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, None, "14.9999999999999999", None, None
                    ),
                },
                datetime.date(year=2001, month=1, day=1),
                datetime.datetime(
                    year=2001,
                    month=1,
                    day=1,
                    hour=14,
                    minute=59,
                    second=59,
                    microsecond=999999,
                ),
            ),
            (
                {
                    "start": DateTuple("2001", "01", "01", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, None, None, "0.00000000999", None
                    ),
                },
                datetime.date(year=2001, month=1, day=1),
                datetime.datetime(year=2001, month=1, day=1),
            ),
            (
                {
                    "start": DateTuple("2001", "01", "01", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, None, None, "0.0000000999", None
                    ),
                },
                datetime.date(year=2001, month=1, day=1),
                datetime.datetime(
                    year=2001, month=1, day=1, hour=0, minute=0, second=0, microsecond=5
                ),
            ),
            (
                {
                    "start": DateTuple("2018", "03", "06", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, None, None, None, "0.0000001"
                    ),
                },
                datetime.date(year=2018, month=3, day=6),
                datetime.datetime(year=2018, month=3, day=6),
            ),
            (
                {
                    "start": DateTuple("2018", "03", "06", None, None, None),
                    "duration": DurationTuple(
                        None, None, None, None, None, None, "2.0000048"
                    ),
                },
                datetime.date(year=2018, month=3, day=6),
                datetime.datetime(
                    year=2018, month=3, day=6, hour=0, minute=0, second=2, microsecond=4
                ),
            ),
            (
                {
                    "start": DatetimeTuple(
                        DateTuple("1980", "03", "05", None, None, None),
                        TimeTuple("01", "01", "00", None),
                    ),
                    "end": DatetimeTuple(
                        DateTuple("1981", "04", "05", None, None, None),
                        TimeTuple("01", "01", "00", None),
                    ),
                },
                datetime.datetime(year=1980, month=3, day=5, hour=1, minute=1),
                datetime.datetime(year=1981, month=4, day=5, hour=1, minute=1),
            ),
            (
                {
                    "start": DatetimeTuple(
                        DateTuple("1980", "03", "05", None, None, None),
                        TimeTuple("01", "01", "00", None),
                    ),
                    "end": DateTuple("1981", "04", "05", None, None, None),
                },
                datetime.datetime(year=1980, month=3, day=5, hour=1, minute=1),
                datetime.date(year=1981, month=4, day=5),
            ),
            (
                {
                    "start": DateTuple("1980", "03", "05", None, None, None),
                    "end": DatetimeTuple(
                        DateTuple("1981", "04", "05", None, None, None),
                        TimeTuple("01", "01", "00", None),
                    ),
                },
                datetime.date(year=1980, month=3, day=5),
                datetime.datetime(year=1981, month=4, day=5, hour=1, minute=1),
            ),
            (
                {
                    "start": DateTuple("1980", "03", "05", None, None, None),
                    "end": DateTuple("1981", "04", "05", None, None, None),
                },
                datetime.date(year=1980, month=3, day=5),
                datetime.date(year=1981, month=4, day=5),
            ),
            (
                {
                    "start": DateTuple("1981", "04", "05", None, None, None),
                    "end": DateTuple("1980", "03", "05", None, None, None),
                },
                datetime.date(year=1981, month=4, day=5),
                datetime.date(year=1980, month=3, day=5),
            ),
            (
                {
                    "start": DatetimeTuple(
                        DateTuple("2050", "03", "01", None, None, None),
                        TimeTuple(
                            "13",
                            "00",
                            "00",
                            TimezoneTuple(False, True, None, None, "Z"),
                        ),
                    ),
                    "end": DatetimeTuple(
                        DateTuple("2050", "05", "11", None, None, None),
                        TimeTuple(
                            "15",
                            "30",
                            "00",
                            TimezoneTuple(False, True, None, None, "Z"),
                        ),
                    ),
                },
                datetime.datetime(
                    year=2050,
                    month=3,
                    day=1,
                    hour=13,
                    tzinfo=UTCOffset(name="UTC", minutes=0),
                ),
                datetime.datetime(
                    year=2050,
                    month=5,
                    day=11,
                    hour=15,
                    minute=30,
                    tzinfo=UTCOffset(name="UTC", minutes=0),
                ),
            ),
            # Test concise representation
            (
                {
                    "start": DateTuple("2020", "01", "01", None, None, None),
                    "end": DateTuple(None, None, "02", None, None, None),
                },
                datetime.date(year=2020, month=1, day=1),
                datetime.date(year=2020, month=1, day=2),
            ),
            (
                {
                    "start": DateTuple("2008", "02", "15", None, None, None),
                    "end": DateTuple(None, "03", "14", None, None, None),
                },
                datetime.date(year=2008, month=2, day=15),
                datetime.date(year=2008, month=3, day=14),
            ),
            (
                {
                    "start": DatetimeTuple(
                        DateTuple("2007", "12", "14", None, None, None),
                        TimeTuple("13", "30", None, None),
                    ),
                    "end": TimeTuple("15", "30", None, None),
                },
                datetime.datetime(year=2007, month=12, day=14, hour=13, minute=30),
                datetime.datetime(year=2007, month=12, day=14, hour=15, minute=30),
            ),
            (
                {
                    "start": DatetimeTuple(
                        DateTuple("2007", "11", "13", None, None, None),
                        TimeTuple("09", "00", None, None),
                    ),
                    "end": DatetimeTuple(
                        DateTuple(None, None, "15", None, None, None),
                        TimeTuple("17", "00", None, None),
                    ),
                },
                datetime.datetime(year=2007, month=11, day=13, hour=9),
                datetime.datetime(year=2007, month=11, day=15, hour=17),
            ),
            (
                {
                    "start": DatetimeTuple(
                        DateTuple("2007", "11", "13", None, None, None),
                        TimeTuple("00", "00", None, None),
                    ),
                    "end": DatetimeTuple(
                        DateTuple(None, None, "16", None, None, None),
                        TimeTuple("00", "00", None, None),
                    ),
                },
                datetime.datetime(year=2007, month=11, day=13),
                datetime.datetime(year=2007, month=11, day=16),
            ),
            (
                {
                    "start": DatetimeTuple(
                        DateTuple("2007", "11", "13", None, None, None),
                        TimeTuple(
                            "09",
                            "00",
                            None,
                            TimezoneTuple(False, True, None, None, "Z"),
                        ),
                    ),
                    "end": DatetimeTuple(
                        DateTuple(None, None, "15", None, None, None),
                        TimeTuple("17", "00", None, None),
                    ),
                },
                datetime.datetime(
                    year=2007,
                    month=11,
                    day=13,
                    hour=9,
                    tzinfo=UTCOffset(name="UTC", minutes=0),
                ),
                datetime.datetime(
                    year=2007,
                    month=11,
                    day=15,
                    hour=17,
                    tzinfo=UTCOffset(name="UTC", minutes=0),
                ),
            ),
            (
                {
                    "start": DatetimeTuple(
                        DateTuple("2007", "11", "13", None, None, None),
                        TimeTuple("09", "00", None, None),
                    ),
                    "end": TimeTuple("12", "34.567", None, None),
                },
                datetime.datetime(year=2007, month=11, day=13, hour=9),
                datetime.datetime(
                    year=2007,
                    month=11,
                    day=13,
                    hour=12,
                    minute=34,
                    second=34,
                    microsecond=20000,
                ),
            ),
            (
                {
                    "start": DateTuple("2007", "11", "13", None, None, None),
                    "end": TimeTuple("12", "34", None, None),
                },
                datetime.date(year=2007, month=11, day=13),
                datetime.datetime(year=2007, month=11, day=13, hour=12, minute=34),
            ),
            # Make sure we truncate, not round
            # https://bitbucket.org/nielsenb/aniso8601/issues/10/sub-microsecond-precision-in-durations-is
            (
                {
                    "start": DatetimeTuple(
                        DateTuple("1980", "03", "05", None, None, None),
                        TimeTuple("01", "01", "00.0000001", None),
                    ),
                    "end": DatetimeTuple(
                        DateTuple("1981", "04", "05", None, None, None),
                        TimeTuple("14", "43", "59.9999997", None),
                    ),
                },
                datetime.datetime(year=1980, month=3, day=5, hour=1, minute=1),
                datetime.datetime(
                    year=1981,
                    month=4,
                    day=5,
                    hour=14,
                    minute=43,
                    second=59,
                    microsecond=999999,
                ),
            ),
        )

        for testtuple in testtuples:
            result = PythonTimeBuilder.build_interval(**testtuple[0])
            self.assertEqual(result[0], testtuple[1])
            self.assertEqual(result[1], testtuple[2])

    def test_build_repeating_interval(self):
        args = {
            "Rnn": "3",
            "interval": IntervalTuple(
                DateTuple("1981", "04", "05", None, None, None),
                None,
                DurationTuple(None, None, None, "1", None, None, None),
            ),
        }
        results = list(PythonTimeBuilder.build_repeating_interval(**args))

        self.assertEqual(results[0], datetime.date(year=1981, month=4, day=5))
        self.assertEqual(results[1], datetime.date(year=1981, month=4, day=6))
        self.assertEqual(results[2], datetime.date(year=1981, month=4, day=7))

        args = {
            "Rnn": "11",
            "interval": IntervalTuple(
                None,
                DatetimeTuple(
                    DateTuple("1980", "03", "05", None, None, None),
                    TimeTuple("01", "01", "00", None),
                ),
                DurationTuple(None, None, None, None, "1", "2", None),
            ),
        }
        results = list(PythonTimeBuilder.build_repeating_interval(**args))

        for dateindex in compat.range(0, 11):
            self.assertEqual(
                results[dateindex],
                datetime.datetime(year=1980, month=3, day=5, hour=1, minute=1)
                - dateindex * datetime.timedelta(hours=1, minutes=2),
            )

        args = {
            "Rnn": "2",
            "interval": IntervalTuple(
                DatetimeTuple(
                    DateTuple("1980", "03", "05", None, None, None),
                    TimeTuple("01", "01", "00", None),
                ),
                DatetimeTuple(
                    DateTuple("1981", "04", "05", None, None, None),
                    TimeTuple("01", "01", "00", None),
                ),
                None,
            ),
        }
        results = list(PythonTimeBuilder.build_repeating_interval(**args))

        self.assertEqual(
            results[0], datetime.datetime(year=1980, month=3, day=5, hour=1, minute=1)
        )
        self.assertEqual(
            results[1], datetime.datetime(year=1981, month=4, day=5, hour=1, minute=1)
        )

        args = {
            "Rnn": "2",
            "interval": IntervalTuple(
                DatetimeTuple(
                    DateTuple("1980", "03", "05", None, None, None),
                    TimeTuple("01", "01", "00", None),
                ),
                DatetimeTuple(
                    DateTuple("1981", "04", "05", None, None, None),
                    TimeTuple("01", "01", "00", None),
                ),
                None,
            ),
        }
        results = list(PythonTimeBuilder.build_repeating_interval(**args))

        self.assertEqual(
            results[0], datetime.datetime(year=1980, month=3, day=5, hour=1, minute=1)
        )
        self.assertEqual(
            results[1], datetime.datetime(year=1981, month=4, day=5, hour=1, minute=1)
        )

        args = {
            "R": True,
            "interval": IntervalTuple(
                None,
                DatetimeTuple(
                    DateTuple("1980", "03", "05", None, None, None),
                    TimeTuple("01", "01", "00", None),
                ),
                DurationTuple(None, None, None, None, "1", "2", None),
            ),
        }
        resultgenerator = PythonTimeBuilder.build_repeating_interval(**args)

        # Test the first 11 generated
        for dateindex in compat.range(0, 11):
            self.assertEqual(
                next(resultgenerator),
                datetime.datetime(year=1980, month=3, day=5, hour=1, minute=1)
                - dateindex * datetime.timedelta(hours=1, minutes=2),
            )

        args = {
            "R": True,
            "interval": IntervalTuple(
                DateTuple("1981", "04", "05", None, None, None),
                None,
                DurationTuple(None, None, None, "1", None, None, None),
            ),
        }
        resultgenerator = PythonTimeBuilder.build_repeating_interval(**args)

        # Test the first 11 generated
        for dateindex in compat.range(0, 11):
            self.assertEqual(
                next(resultgenerator),
                (
                    datetime.datetime(year=1981, month=4, day=5, hour=0, minute=0)
                    + dateindex * datetime.timedelta(days=1)
                ).date(),
            )

    def test_build_timezone(self):
        testtuples = (
            ({"Z": True, "name": "Z"}, datetime.timedelta(hours=0), "UTC"),
            (
                {"negative": False, "hh": "00", "mm": "00", "name": "+00:00"},
                datetime.timedelta(hours=0),
                "+00:00",
            ),
            (
                {"negative": False, "hh": "01", "mm": "00", "name": "+01:00"},
                datetime.timedelta(hours=1),
                "+01:00",
            ),
            (
                {"negative": True, "hh": "01", "mm": "00", "name": "-01:00"},
                -datetime.timedelta(hours=1),
                "-01:00",
            ),
            (
                {"negative": False, "hh": "00", "mm": "12", "name": "+00:12"},
                datetime.timedelta(minutes=12),
                "+00:12",
            ),
            (
                {"negative": False, "hh": "01", "mm": "23", "name": "+01:23"},
                datetime.timedelta(hours=1, minutes=23),
                "+01:23",
            ),
            (
                {"negative": True, "hh": "01", "mm": "23", "name": "-01:23"},
                -datetime.timedelta(hours=1, minutes=23),
                "-01:23",
            ),
            (
                {"negative": False, "hh": "00", "name": "+00"},
                datetime.timedelta(hours=0),
                "+00",
            ),
            (
                {"negative": False, "hh": "01", "name": "+01"},
                datetime.timedelta(hours=1),
                "+01",
            ),
            (
                {"negative": True, "hh": "01", "name": "-01"},
                -datetime.timedelta(hours=1),
                "-01",
            ),
            (
                {"negative": False, "hh": "12", "name": "+12"},
                datetime.timedelta(hours=12),
                "+12",
            ),
            (
                {"negative": True, "hh": "12", "name": "-12"},
                -datetime.timedelta(hours=12),
                "-12",
            ),
        )

        for testtuple in testtuples:
            result = PythonTimeBuilder.build_timezone(**testtuple[0])
            self.assertEqual(result.utcoffset(None), testtuple[1])
            self.assertEqual(result.tzname(None), testtuple[2])

    def test_range_check_date(self):
        # 0 isn't a valid year for a Python builder
        with self.assertRaises(YearOutOfBoundsError):
            PythonTimeBuilder.build_date(YYYY="0000")

        # Leap year
        # https://bitbucket.org/nielsenb/aniso8601/issues/14/parsing-ordinal-dates-should-only-allow
        with self.assertRaises(DayOutOfBoundsError):
            PythonTimeBuilder.build_date(YYYY="1981", DDD="366")

    def test_range_check_time(self):
        # Hour 24 can only represent midnight
        with self.assertRaises(MidnightBoundsError):
            PythonTimeBuilder.build_time(hh="24", mm="00", ss="01")

        with self.assertRaises(MidnightBoundsError):
            PythonTimeBuilder.build_time(hh="24", mm="00.1")

        with self.assertRaises(MidnightBoundsError):
            PythonTimeBuilder.build_time(hh="24", mm="01")

        with self.assertRaises(MidnightBoundsError):
            PythonTimeBuilder.build_time(hh="24.1")

    def test_range_check_duration(self):
        with self.assertRaises(YearOutOfBoundsError):
            PythonTimeBuilder.build_duration(
                PnY=str((datetime.timedelta.max.days // 365) + 1)
            )

        with self.assertRaises(MonthOutOfBoundsError):
            PythonTimeBuilder.build_duration(
                PnM=str((datetime.timedelta.max.days // 30) + 1)
            )

        with self.assertRaises(DayOutOfBoundsError):
            PythonTimeBuilder.build_duration(PnD=str(datetime.timedelta.max.days + 1))

        with self.assertRaises(WeekOutOfBoundsError):
            PythonTimeBuilder.build_duration(
                PnW=str((datetime.timedelta.max.days // 7) + 1)
            )

        with self.assertRaises(HoursOutOfBoundsError):
            PythonTimeBuilder.build_duration(
                TnH=str((datetime.timedelta.max.days * 24) + 24)
            )

        with self.assertRaises(MinutesOutOfBoundsError):
            PythonTimeBuilder.build_duration(
                TnM=str((datetime.timedelta.max.days * 24 * 60) + 24 * 60)
            )

        with self.assertRaises(SecondsOutOfBoundsError):
            PythonTimeBuilder.build_duration(
                TnS=str((datetime.timedelta.max.days * 24 * 60 * 60) + 24 * 60 * 60)
            )

        # Split max range across all parts
        maxpart = datetime.timedelta.max.days // 7

        with self.assertRaises(DayOutOfBoundsError):
            PythonTimeBuilder.build_duration(
                PnY=str((maxpart // 365) + 1),
                PnM=str((maxpart // 30) + 1),
                PnD=str((maxpart + 1)),
                PnW=str((maxpart // 7) + 1),
                TnH=str((maxpart * 24) + 1),
                TnM=str((maxpart * 24 * 60) + 1),
                TnS=str((maxpart * 24 * 60 * 60) + 1),
            )

    def test_range_check_interval(self):
        with self.assertRaises(YearOutOfBoundsError):
            PythonTimeBuilder.build_interval(
                start=DateTuple("0007", None, None, None, None, None),
                duration=DurationTuple(
                    None, None, None, str(datetime.timedelta.max.days), None, None, None
                ),
            )

        with self.assertRaises(YearOutOfBoundsError):
            PythonTimeBuilder.build_interval(
                start=DatetimeTuple(
                    DateTuple("0007", None, None, None, None, None),
                    TimeTuple("1", None, None, None),
                ),
                duration=DurationTuple(
                    str(datetime.timedelta.max.days // 365),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
            )

        with self.assertRaises(YearOutOfBoundsError):
            PythonTimeBuilder.build_interval(
                end=DateTuple("0001", None, None, None, None, None),
                duration=DurationTuple("3", None, None, None, None, None, None),
            )

        with self.assertRaises(YearOutOfBoundsError):
            PythonTimeBuilder.build_interval(
                end=DatetimeTuple(
                    DateTuple("0001", None, None, None, None, None),
                    TimeTuple("1", None, None, None),
                ),
                duration=DurationTuple("2", None, None, None, None, None, None),
            )

    def test_build_week_date(self):
        weekdate = PythonTimeBuilder._build_week_date(2009, 1)
        self.assertEqual(weekdate, datetime.date(year=2008, month=12, day=29))

        weekdate = PythonTimeBuilder._build_week_date(2009, 53, isoday=7)
        self.assertEqual(weekdate, datetime.date(year=2010, month=1, day=3))

    def test_build_ordinal_date(self):
        ordinaldate = PythonTimeBuilder._build_ordinal_date(1981, 95)
        self.assertEqual(ordinaldate, datetime.date(year=1981, month=4, day=5))

    def test_iso_year_start(self):
        yearstart = PythonTimeBuilder._iso_year_start(2004)
        self.assertEqual(yearstart, datetime.date(year=2003, month=12, day=29))

        yearstart = PythonTimeBuilder._iso_year_start(2010)
        self.assertEqual(yearstart, datetime.date(year=2010, month=1, day=4))

        yearstart = PythonTimeBuilder._iso_year_start(2009)
        self.assertEqual(yearstart, datetime.date(year=2008, month=12, day=29))

    def test_date_generator(self):
        startdate = datetime.date(year=2018, month=8, day=29)
        timedelta = datetime.timedelta(days=1)
        iterations = 10

        generator = PythonTimeBuilder._date_generator(startdate, timedelta, iterations)

        results = list(generator)

        for dateindex in compat.range(0, 10):
            self.assertEqual(
                results[dateindex],
                datetime.date(year=2018, month=8, day=29)
                + dateindex * datetime.timedelta(days=1),
            )

    def test_date_generator_unbounded(self):
        startdate = datetime.date(year=2018, month=8, day=29)
        timedelta = datetime.timedelta(days=5)

        generator = PythonTimeBuilder._date_generator_unbounded(startdate, timedelta)

        # Check the first 10 results
        for dateindex in compat.range(0, 10):
            self.assertEqual(
                next(generator),
                datetime.date(year=2018, month=8, day=29)
                + dateindex * datetime.timedelta(days=5),
            )

    def test_distribute_microseconds(self):
        self.assertEqual(PythonTimeBuilder._distribute_microseconds(1, (), ()), (1,))
        self.assertEqual(
            PythonTimeBuilder._distribute_microseconds(11, (0,), (10,)), (1, 1)
        )
        self.assertEqual(
            PythonTimeBuilder._distribute_microseconds(211, (0, 0), (100, 10)),
            (2, 1, 1),
        )

        self.assertEqual(PythonTimeBuilder._distribute_microseconds(1, (), ()), (1,))
        self.assertEqual(
            PythonTimeBuilder._distribute_microseconds(11, (5,), (10,)), (6, 1)
        )
        self.assertEqual(
            PythonTimeBuilder._distribute_microseconds(211, (10, 5), (100, 10)),
            (12, 6, 1),
        )
