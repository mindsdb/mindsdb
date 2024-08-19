# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

import unittest

import aniso8601
from aniso8601.date import get_date_resolution, parse_date
from aniso8601.exceptions import DayOutOfBoundsError, ISOFormatError
from aniso8601.resolution import DateResolution
from aniso8601.tests.compat import mock


class TestDateResolutionFunctions(unittest.TestCase):
    def test_get_date_resolution_year(self):
        self.assertEqual(get_date_resolution("2013"), DateResolution.Year)
        self.assertEqual(get_date_resolution("0001"), DateResolution.Year)
        self.assertEqual(get_date_resolution("19"), DateResolution.Year)

    def test_get_date_resolution_month(self):
        self.assertEqual(get_date_resolution("1981-04"), DateResolution.Month)

    def test_get_date_resolution_week(self):
        self.assertEqual(get_date_resolution("2004-W53"), DateResolution.Week)
        self.assertEqual(get_date_resolution("2009-W01"), DateResolution.Week)
        self.assertEqual(get_date_resolution("2004W53"), DateResolution.Week)

    def test_get_date_resolution_day(self):
        self.assertEqual(get_date_resolution("2004-04-11"), DateResolution.Day)
        self.assertEqual(get_date_resolution("20090121"), DateResolution.Day)

    def test_get_date_resolution_year_weekday(self):
        self.assertEqual(get_date_resolution("2004-W53-6"), DateResolution.Weekday)
        self.assertEqual(get_date_resolution("2004W536"), DateResolution.Weekday)

    def test_get_date_resolution_year_ordinal(self):
        self.assertEqual(get_date_resolution("1981-095"), DateResolution.Ordinal)
        self.assertEqual(get_date_resolution("1981095"), DateResolution.Ordinal)

    def test_get_date_resolution_badtype(self):
        testtuples = (None, 1, False, 1.234)

        for testtuple in testtuples:
            with self.assertRaises(ValueError):
                get_date_resolution(testtuple)

    def test_get_date_resolution_extended_year(self):
        testtuples = ("+2000", "+30000")

        for testtuple in testtuples:
            with self.assertRaises(NotImplementedError):
                get_date_resolution(testtuple)

    def test_get_date_resolution_badweek(self):
        testtuples = ("2004-W1", "2004W1")

        for testtuple in testtuples:
            with self.assertRaises(ISOFormatError):
                get_date_resolution(testtuple)

    def test_get_date_resolution_badweekday(self):
        testtuples = ("2004-W53-67", "2004W5367")

        for testtuple in testtuples:
            with self.assertRaises(ISOFormatError):
                get_date_resolution(testtuple)

    def test_get_date_resolution_badstr(self):
        testtuples = (
            "W53",
            "2004-W",
            "2014-01-230",
            "2014-012-23",
            "201-01-23",
            "201401230",
            "201401",
            "",
        )

        for testtuple in testtuples:
            with self.assertRaises(ISOFormatError):
                get_date_resolution(testtuple)


class TestDateParserFunctions(unittest.TestCase):
    def test_parse_date(self):
        testtuples = (
            (
                "2013",
                {
                    "YYYY": "2013",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
            ),
            (
                "0001",
                {
                    "YYYY": "0001",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
            ),
            (
                "19",
                {
                    "YYYY": "19",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
            ),
            (
                "1981-04-05",
                {
                    "YYYY": "1981",
                    "MM": "04",
                    "DD": "05",
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
            ),
            (
                "19810405",
                {
                    "YYYY": "1981",
                    "MM": "04",
                    "DD": "05",
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
            ),
            (
                "1981-04",
                {
                    "YYYY": "1981",
                    "MM": "04",
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": None,
                },
            ),
            (
                "2004-W53",
                {
                    "YYYY": "2004",
                    "MM": None,
                    "DD": None,
                    "Www": "53",
                    "D": None,
                    "DDD": None,
                },
            ),
            (
                "2009-W01",
                {
                    "YYYY": "2009",
                    "MM": None,
                    "DD": None,
                    "Www": "01",
                    "D": None,
                    "DDD": None,
                },
            ),
            (
                "2004-W53-6",
                {
                    "YYYY": "2004",
                    "MM": None,
                    "DD": None,
                    "Www": "53",
                    "D": "6",
                    "DDD": None,
                },
            ),
            (
                "2004W53",
                {
                    "YYYY": "2004",
                    "MM": None,
                    "DD": None,
                    "Www": "53",
                    "D": None,
                    "DDD": None,
                },
            ),
            (
                "2004W536",
                {
                    "YYYY": "2004",
                    "MM": None,
                    "DD": None,
                    "Www": "53",
                    "D": "6",
                    "DDD": None,
                },
            ),
            (
                "1981-095",
                {
                    "YYYY": "1981",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": "095",
                },
            ),
            (
                "1981095",
                {
                    "YYYY": "1981",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": "095",
                },
            ),
            (
                "1980366",
                {
                    "YYYY": "1980",
                    "MM": None,
                    "DD": None,
                    "Www": None,
                    "D": None,
                    "DDD": "366",
                },
            ),
        )

        for testtuple in testtuples:
            with mock.patch.object(
                aniso8601.date.PythonTimeBuilder, "build_date"
            ) as mockBuildDate:
                mockBuildDate.return_value = testtuple[1]

                result = parse_date(testtuple[0])

                self.assertEqual(result, testtuple[1])
                mockBuildDate.assert_called_once_with(**testtuple[1])

    def test_parse_date_badtype(self):
        testtuples = (None, 1, False, 1.234)

        for testtuple in testtuples:
            with self.assertRaises(ValueError):
                parse_date(testtuple, builder=None)

    def test_parse_date_badstr(self):
        testtuples = (
            "W53",
            "2004-W",
            "2014-01-230",
            "2014-012-23",
            "201-01-23",
            "201401230",
            "201401",
            "9999 W53",
            "20.50230",
            "198104",
            "bad",
            "",
        )

        for testtuple in testtuples:
            with self.assertRaises(ISOFormatError):
                parse_date(testtuple, builder=None)

    def test_parse_date_mockbuilder(self):
        mockBuilder = mock.Mock()

        expectedargs = {
            "YYYY": "1981",
            "MM": "04",
            "DD": "05",
            "Www": None,
            "D": None,
            "DDD": None,
        }

        mockBuilder.build_date.return_value = expectedargs

        result = parse_date("1981-04-05", builder=mockBuilder)

        self.assertEqual(result, expectedargs)
        mockBuilder.build_date.assert_called_once_with(**expectedargs)
