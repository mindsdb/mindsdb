# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

import unittest

import aniso8601
from aniso8601.exceptions import ISOFormatError
from aniso8601.tests.compat import mock
from aniso8601.timezone import parse_timezone


class TestTimezoneParserFunctions(unittest.TestCase):
    def test_parse_timezone(self):
        testtuples = (
            ("Z", {"negative": False, "Z": True, "name": "Z"}),
            ("+00:00", {"negative": False, "hh": "00", "mm": "00", "name": "+00:00"}),
            ("+01:00", {"negative": False, "hh": "01", "mm": "00", "name": "+01:00"}),
            ("-01:00", {"negative": True, "hh": "01", "mm": "00", "name": "-01:00"}),
            ("+00:12", {"negative": False, "hh": "00", "mm": "12", "name": "+00:12"}),
            ("+01:23", {"negative": False, "hh": "01", "mm": "23", "name": "+01:23"}),
            ("-01:23", {"negative": True, "hh": "01", "mm": "23", "name": "-01:23"}),
            ("+0000", {"negative": False, "hh": "00", "mm": "00", "name": "+0000"}),
            ("+0100", {"negative": False, "hh": "01", "mm": "00", "name": "+0100"}),
            ("-0100", {"negative": True, "hh": "01", "mm": "00", "name": "-0100"}),
            ("+0012", {"negative": False, "hh": "00", "mm": "12", "name": "+0012"}),
            ("+0123", {"negative": False, "hh": "01", "mm": "23", "name": "+0123"}),
            ("-0123", {"negative": True, "hh": "01", "mm": "23", "name": "-0123"}),
            ("+00", {"negative": False, "hh": "00", "mm": None, "name": "+00"}),
            ("+01", {"negative": False, "hh": "01", "mm": None, "name": "+01"}),
            ("-01", {"negative": True, "hh": "01", "mm": None, "name": "-01"}),
            ("+12", {"negative": False, "hh": "12", "mm": None, "name": "+12"}),
            ("-12", {"negative": True, "hh": "12", "mm": None, "name": "-12"}),
        )

        for testtuple in testtuples:
            with mock.patch.object(
                aniso8601.timezone.PythonTimeBuilder, "build_timezone"
            ) as mockBuildTimezone:

                mockBuildTimezone.return_value = testtuple[1]

                result = parse_timezone(testtuple[0])

                self.assertEqual(result, testtuple[1])
                mockBuildTimezone.assert_called_once_with(**testtuple[1])

    def test_parse_timezone_badtype(self):
        testtuples = (None, 1, False, 1.234)

        for testtuple in testtuples:
            with self.assertRaises(ValueError):
                parse_timezone(testtuple, builder=None)

    def test_parse_timezone_badstr(self):
        testtuples = (
            "+1",
            "-00",
            "-0000",
            "-00:00",
            "01",
            "0123",
            "@12:34",
            "Y",
            " Z",
            "Z ",
            " Z ",
            "bad",
            "",
        )

        for testtuple in testtuples:
            with self.assertRaises(ISOFormatError):
                parse_timezone(testtuple, builder=None)

    def test_parse_timezone_mockbuilder(self):
        mockBuilder = mock.Mock()

        expectedargs = {"negative": False, "Z": True, "name": "Z"}

        mockBuilder.build_timezone.return_value = expectedargs

        result = parse_timezone("Z", builder=mockBuilder)

        self.assertEqual(result, expectedargs)
        mockBuilder.build_timezone.assert_called_once_with(**expectedargs)

        mockBuilder = mock.Mock()

        expectedargs = {"negative": False, "hh": "00", "mm": "00", "name": "+00:00"}

        mockBuilder.build_timezone.return_value = expectedargs

        result = parse_timezone("+00:00", builder=mockBuilder)

        self.assertEqual(result, expectedargs)
        mockBuilder.build_timezone.assert_called_once_with(**expectedargs)

        mockBuilder = mock.Mock()

        expectedargs = {"negative": True, "hh": "01", "mm": "23", "name": "-01:23"}

        mockBuilder.build_timezone.return_value = expectedargs

        result = parse_timezone("-01:23", builder=mockBuilder)

        self.assertEqual(result, expectedargs)
        mockBuilder.build_timezone.assert_called_once_with(**expectedargs)

    def test_parse_timezone_negativezero(self):
        # A 0 offset cannot be negative
        with self.assertRaises(ISOFormatError):
            parse_timezone("-00:00", builder=None)

        with self.assertRaises(ISOFormatError):
            parse_timezone("-0000", builder=None)

        with self.assertRaises(ISOFormatError):
            parse_timezone("-00", builder=None)
