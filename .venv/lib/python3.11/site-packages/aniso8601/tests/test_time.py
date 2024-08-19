# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

import unittest

import aniso8601
from aniso8601.builders import DatetimeTuple, DateTuple, TimeTuple, TimezoneTuple
from aniso8601.exceptions import ISOFormatError
from aniso8601.resolution import TimeResolution
from aniso8601.tests.compat import mock
from aniso8601.time import (
    _get_time_resolution,
    get_datetime_resolution,
    get_time_resolution,
    parse_datetime,
    parse_time,
)


class TestTimeResolutionFunctions(unittest.TestCase):
    def test_get_time_resolution(self):
        self.assertEqual(get_time_resolution("01:23:45"), TimeResolution.Seconds)
        self.assertEqual(get_time_resolution("24:00:00"), TimeResolution.Seconds)
        self.assertEqual(get_time_resolution("23:21:28,512400"), TimeResolution.Seconds)
        self.assertEqual(get_time_resolution("23:21:28.512400"), TimeResolution.Seconds)
        self.assertEqual(get_time_resolution("01:23"), TimeResolution.Minutes)
        self.assertEqual(get_time_resolution("24:00"), TimeResolution.Minutes)
        self.assertEqual(get_time_resolution("01:23,4567"), TimeResolution.Minutes)
        self.assertEqual(get_time_resolution("01:23.4567"), TimeResolution.Minutes)
        self.assertEqual(get_time_resolution("012345"), TimeResolution.Seconds)
        self.assertEqual(get_time_resolution("240000"), TimeResolution.Seconds)
        self.assertEqual(get_time_resolution("0123"), TimeResolution.Minutes)
        self.assertEqual(get_time_resolution("2400"), TimeResolution.Minutes)
        self.assertEqual(get_time_resolution("01"), TimeResolution.Hours)
        self.assertEqual(get_time_resolution("24"), TimeResolution.Hours)
        self.assertEqual(get_time_resolution("12,5"), TimeResolution.Hours)
        self.assertEqual(get_time_resolution("12.5"), TimeResolution.Hours)
        self.assertEqual(
            get_time_resolution("232128.512400+00:00"), TimeResolution.Seconds
        )
        self.assertEqual(get_time_resolution("0123.4567+00:00"), TimeResolution.Minutes)
        self.assertEqual(get_time_resolution("01.4567+00:00"), TimeResolution.Hours)
        self.assertEqual(get_time_resolution("01:23:45+00:00"), TimeResolution.Seconds)
        self.assertEqual(get_time_resolution("24:00:00+00:00"), TimeResolution.Seconds)
        self.assertEqual(
            get_time_resolution("23:21:28.512400+00:00"), TimeResolution.Seconds
        )
        self.assertEqual(get_time_resolution("01:23+00:00"), TimeResolution.Minutes)
        self.assertEqual(get_time_resolution("24:00+00:00"), TimeResolution.Minutes)
        self.assertEqual(
            get_time_resolution("01:23.4567+00:00"), TimeResolution.Minutes
        )
        self.assertEqual(
            get_time_resolution("23:21:28.512400+11:15"), TimeResolution.Seconds
        )
        self.assertEqual(
            get_time_resolution("23:21:28.512400-12:34"), TimeResolution.Seconds
        )
        self.assertEqual(
            get_time_resolution("23:21:28.512400Z"), TimeResolution.Seconds
        )
        self.assertEqual(
            get_time_resolution("06:14:00.000123Z"), TimeResolution.Seconds
        )

    def test_get_datetime_resolution(self):
        self.assertEqual(
            get_datetime_resolution("2019-06-05T01:03:11.858714"),
            TimeResolution.Seconds,
        )
        self.assertEqual(
            get_datetime_resolution("2019-06-05T01:03:11"), TimeResolution.Seconds
        )
        self.assertEqual(
            get_datetime_resolution("2019-06-05T01:03"), TimeResolution.Minutes
        )
        self.assertEqual(get_datetime_resolution("2019-06-05T01"), TimeResolution.Hours)

    def test_get_time_resolution_badtype(self):
        testtuples = (None, 1, False, 1.234)

        for testtuple in testtuples:
            with self.assertRaises(ValueError):
                get_time_resolution(testtuple)

    def test_get_time_resolution_badstr(self):
        testtuples = ("A6:14:00.000123Z", "06:14:0B", "bad", "")

        for testtuple in testtuples:
            with self.assertRaises(ISOFormatError):
                get_time_resolution(testtuple)

    def test_get_time_resolution_internal(self):
        self.assertEqual(
            _get_time_resolution(TimeTuple(hh="01", mm="02", ss="03", tz=None)),
            TimeResolution.Seconds,
        )
        self.assertEqual(
            _get_time_resolution(TimeTuple(hh="01", mm="02", ss=None, tz=None)),
            TimeResolution.Minutes,
        )
        self.assertEqual(
            _get_time_resolution(TimeTuple(hh="01", mm=None, ss=None, tz=None)),
            TimeResolution.Hours,
        )


class TestTimeParserFunctions(unittest.TestCase):
    def test_parse_time(self):
        testtuples = (
            ("01:23:45", {"hh": "01", "mm": "23", "ss": "45", "tz": None}),
            ("24:00:00", {"hh": "24", "mm": "00", "ss": "00", "tz": None}),
            (
                "23:21:28,512400",
                {"hh": "23", "mm": "21", "ss": "28.512400", "tz": None},
            ),
            (
                "23:21:28.512400",
                {"hh": "23", "mm": "21", "ss": "28.512400", "tz": None},
            ),
            (
                "01:03:11.858714",
                {"hh": "01", "mm": "03", "ss": "11.858714", "tz": None},
            ),
            (
                "14:43:59.9999997",
                {"hh": "14", "mm": "43", "ss": "59.9999997", "tz": None},
            ),
            ("01:23", {"hh": "01", "mm": "23", "ss": None, "tz": None}),
            ("24:00", {"hh": "24", "mm": "00", "ss": None, "tz": None}),
            ("01:23,4567", {"hh": "01", "mm": "23.4567", "ss": None, "tz": None}),
            ("01:23.4567", {"hh": "01", "mm": "23.4567", "ss": None, "tz": None}),
            ("012345", {"hh": "01", "mm": "23", "ss": "45", "tz": None}),
            ("240000", {"hh": "24", "mm": "00", "ss": "00", "tz": None}),
            ("232128,512400", {"hh": "23", "mm": "21", "ss": "28.512400", "tz": None}),
            ("232128.512400", {"hh": "23", "mm": "21", "ss": "28.512400", "tz": None}),
            ("010311.858714", {"hh": "01", "mm": "03", "ss": "11.858714", "tz": None}),
            (
                "144359.9999997",
                {"hh": "14", "mm": "43", "ss": "59.9999997", "tz": None},
            ),
            ("0123", {"hh": "01", "mm": "23", "ss": None, "tz": None}),
            ("2400", {"hh": "24", "mm": "00", "ss": None, "tz": None}),
            ("01", {"hh": "01", "mm": None, "ss": None, "tz": None}),
            ("24", {"hh": "24", "mm": None, "ss": None, "tz": None}),
            ("12,5", {"hh": "12.5", "mm": None, "ss": None, "tz": None}),
            ("12.5", {"hh": "12.5", "mm": None, "ss": None, "tz": None}),
            (
                "232128,512400+00:00",
                {
                    "hh": "23",
                    "mm": "21",
                    "ss": "28.512400",
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "232128.512400+00:00",
                {
                    "hh": "23",
                    "mm": "21",
                    "ss": "28.512400",
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "0123,4567+00:00",
                {
                    "hh": "01",
                    "mm": "23.4567",
                    "ss": None,
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "0123.4567+00:00",
                {
                    "hh": "01",
                    "mm": "23.4567",
                    "ss": None,
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "01,4567+00:00",
                {
                    "hh": "01.4567",
                    "mm": None,
                    "ss": None,
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "01.4567+00:00",
                {
                    "hh": "01.4567",
                    "mm": None,
                    "ss": None,
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "01:23:45+00:00",
                {
                    "hh": "01",
                    "mm": "23",
                    "ss": "45",
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "24:00:00+00:00",
                {
                    "hh": "24",
                    "mm": "00",
                    "ss": "00",
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "23:21:28.512400+00:00",
                {
                    "hh": "23",
                    "mm": "21",
                    "ss": "28.512400",
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "01:23+00:00",
                {
                    "hh": "01",
                    "mm": "23",
                    "ss": None,
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "24:00+00:00",
                {
                    "hh": "24",
                    "mm": "00",
                    "ss": None,
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "01:23.4567+00:00",
                {
                    "hh": "01",
                    "mm": "23.4567",
                    "ss": None,
                    "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
                },
            ),
            (
                "23:21:28.512400+11:15",
                {
                    "hh": "23",
                    "mm": "21",
                    "ss": "28.512400",
                    "tz": TimezoneTuple(False, None, "11", "15", "+11:15"),
                },
            ),
            (
                "23:21:28.512400-12:34",
                {
                    "hh": "23",
                    "mm": "21",
                    "ss": "28.512400",
                    "tz": TimezoneTuple(True, None, "12", "34", "-12:34"),
                },
            ),
            (
                "23:21:28.512400Z",
                {
                    "hh": "23",
                    "mm": "21",
                    "ss": "28.512400",
                    "tz": TimezoneTuple(False, True, None, None, "Z"),
                },
            ),
            (
                "06:14:00.000123Z",
                {
                    "hh": "06",
                    "mm": "14",
                    "ss": "00.000123",
                    "tz": TimezoneTuple(False, True, None, None, "Z"),
                },
            ),
        )

        for testtuple in testtuples:
            with mock.patch.object(
                aniso8601.time.PythonTimeBuilder, "build_time"
            ) as mockBuildTime:

                mockBuildTime.return_value = testtuple[1]

                result = parse_time(testtuple[0])

                self.assertEqual(result, testtuple[1])
                mockBuildTime.assert_called_once_with(**testtuple[1])

    def test_parse_time_badtype(self):
        testtuples = (None, 1, False, 1.234)

        for testtuple in testtuples:
            with self.assertRaises(ValueError):
                parse_time(testtuple, builder=None)

    def test_parse_time_badstr(self):
        testtuples = (
            "A6:14:00.000123Z",
            "06:14:0B",
            "06:1 :02",
            "0000,70:24,9",
            "00.27:5332",
            "bad",
            "",
        )

        for testtuple in testtuples:
            with self.assertRaises(ISOFormatError):
                parse_time(testtuple, builder=None)

    def test_parse_time_mockbuilder(self):
        mockBuilder = mock.Mock()

        expectedargs = {"hh": "01", "mm": "23", "ss": "45", "tz": None}

        mockBuilder.build_time.return_value = expectedargs

        result = parse_time("01:23:45", builder=mockBuilder)

        self.assertEqual(result, expectedargs)
        mockBuilder.build_time.assert_called_once_with(**expectedargs)

        mockBuilder = mock.Mock()

        expectedargs = {
            "hh": "23",
            "mm": "21",
            "ss": "28.512400",
            "tz": TimezoneTuple(False, None, "00", "00", "+00:00"),
        }

        mockBuilder.build_time.return_value = expectedargs

        result = parse_time("232128.512400+00:00", builder=mockBuilder)

        self.assertEqual(result, expectedargs)
        mockBuilder.build_time.assert_called_once_with(**expectedargs)

        mockBuilder = mock.Mock()

        expectedargs = {
            "hh": "23",
            "mm": "21",
            "ss": "28.512400",
            "tz": TimezoneTuple(False, None, "11", "15", "+11:15"),
        }

        mockBuilder.build_time.return_value = expectedargs

        result = parse_time("23:21:28.512400+11:15", builder=mockBuilder)

        self.assertEqual(result, expectedargs)
        mockBuilder.build_time.assert_called_once_with(**expectedargs)

    def test_parse_datetime(self):
        testtuples = (
            (
                "2019-06-05T01:03:11,858714",
                (
                    DateTuple("2019", "06", "05", None, None, None),
                    TimeTuple("01", "03", "11.858714", None),
                ),
            ),
            (
                "2019-06-05T01:03:11.858714",
                (
                    DateTuple("2019", "06", "05", None, None, None),
                    TimeTuple("01", "03", "11.858714", None),
                ),
            ),
            (
                "1981-04-05T23:21:28.512400Z",
                (
                    DateTuple("1981", "04", "05", None, None, None),
                    TimeTuple(
                        "23",
                        "21",
                        "28.512400",
                        TimezoneTuple(False, True, None, None, "Z"),
                    ),
                ),
            ),
            (
                "1981095T23:21:28.512400-12:34",
                (
                    DateTuple("1981", None, None, None, None, "095"),
                    TimeTuple(
                        "23",
                        "21",
                        "28.512400",
                        TimezoneTuple(True, None, "12", "34", "-12:34"),
                    ),
                ),
            ),
            (
                "19810405T23:21:28+00",
                (
                    DateTuple("1981", "04", "05", None, None, None),
                    TimeTuple(
                        "23", "21", "28", TimezoneTuple(False, None, "00", None, "+00")
                    ),
                ),
            ),
            (
                "19810405T23:21:28+00:00",
                (
                    DateTuple("1981", "04", "05", None, None, None),
                    TimeTuple(
                        "23",
                        "21",
                        "28",
                        TimezoneTuple(False, None, "00", "00", "+00:00"),
                    ),
                ),
            ),
        )

        for testtuple in testtuples:
            with mock.patch.object(
                aniso8601.time.PythonTimeBuilder, "build_datetime"
            ) as mockBuildDateTime:

                mockBuildDateTime.return_value = testtuple[1]

                result = parse_datetime(testtuple[0])

            self.assertEqual(result, testtuple[1])
            mockBuildDateTime.assert_called_once_with(*testtuple[1])

    def test_parse_datetime_spacedelimited(self):
        expectedargs = (
            DateTuple("2004", None, None, "53", "6", None),
            TimeTuple(
                "23", "21", "28.512400", TimezoneTuple(True, None, "12", "34", "-12:34")
            ),
        )

        with mock.patch.object(
            aniso8601.time.PythonTimeBuilder, "build_datetime"
        ) as mockBuildDateTime:

            mockBuildDateTime.return_value = expectedargs

            result = parse_datetime("2004-W53-6 23:21:28.512400-12:34", delimiter=" ")

        self.assertEqual(result, expectedargs)
        mockBuildDateTime.assert_called_once_with(*expectedargs)

    def test_parse_datetime_commadelimited(self):
        expectedargs = (
            DateTuple("1981", "04", "05", None, None, None),
            TimeTuple(
                "23", "21", "28.512400", TimezoneTuple(False, True, None, None, "Z")
            ),
        )

        with mock.patch.object(
            aniso8601.time.PythonTimeBuilder, "build_datetime"
        ) as mockBuildDateTime:

            mockBuildDateTime.return_value = expectedargs

            result = parse_datetime("1981-04-05,23:21:28,512400Z", delimiter=",")

        self.assertEqual(result, expectedargs)
        mockBuildDateTime.assert_called_once_with(*expectedargs)

    def test_parse_datetime_baddelimiter(self):
        testtuples = (
            "1981-04-05,23:21:28,512400Z",
            "2004-W53-6 23:21:28.512400-12:3",
            "1981040523:21:28",
        )

        for testtuple in testtuples:
            with self.assertRaises(ISOFormatError):
                parse_datetime(testtuple, builder=None)

    def test_parse_datetime_badtype(self):
        testtuples = (None, 1, False, 1.234)

        for testtuple in testtuples:
            with self.assertRaises(ValueError):
                parse_datetime(testtuple, builder=None)

    def test_parse_datetime_badstr(self):
        testtuples = (
            "1981-04-05TA6:14:00.000123Z",
            "2004-W53-6T06:14:0B",
            "2014-01-230T23:21:28+00",
            "201401230T01:03:11.858714",
            "9999 W53T49",
            "9T0000,70:24,9",
            "bad",
            "",
        )

        for testtuple in testtuples:
            with self.assertRaises(ISOFormatError):
                parse_datetime(testtuple, builder=None)

    def test_parse_datetime_mockbuilder(self):
        mockBuilder = mock.Mock()

        expectedargs = (
            DateTuple("1981", None, None, None, None, "095"),
            TimeTuple(
                "23", "21", "28.512400", TimezoneTuple(True, None, "12", "34", "-12:34")
            ),
        )

        mockBuilder.build_datetime.return_value = expectedargs

        result = parse_datetime("1981095T23:21:28.512400-12:34", builder=mockBuilder)

        self.assertEqual(result, expectedargs)
        mockBuilder.build_datetime.assert_called_once_with(*expectedargs)
