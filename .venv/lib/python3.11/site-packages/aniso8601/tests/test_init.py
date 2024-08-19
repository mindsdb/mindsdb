# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

import unittest

import aniso8601


class TestInitFunctions(unittest.TestCase):
    def test_import(self):
        # Verify the function mappings
        self.assertEqual(aniso8601.parse_datetime, aniso8601.time.parse_datetime)
        self.assertEqual(aniso8601.parse_time, aniso8601.time.parse_time)
        self.assertEqual(
            aniso8601.get_time_resolution, aniso8601.time.get_time_resolution
        )
        self.assertEqual(
            aniso8601.get_datetime_resolution, aniso8601.time.get_datetime_resolution
        )

        self.assertEqual(aniso8601.parse_date, aniso8601.date.parse_date)
        self.assertEqual(
            aniso8601.get_date_resolution, aniso8601.date.get_date_resolution
        )

        self.assertEqual(aniso8601.parse_duration, aniso8601.duration.parse_duration)
        self.assertEqual(
            aniso8601.get_duration_resolution,
            aniso8601.duration.get_duration_resolution,
        )

        self.assertEqual(aniso8601.parse_interval, aniso8601.interval.parse_interval)
        self.assertEqual(
            aniso8601.parse_repeating_interval,
            aniso8601.interval.parse_repeating_interval,
        )
        self.assertEqual(
            aniso8601.get_interval_resolution,
            aniso8601.interval.get_interval_resolution,
        )
        self.assertEqual(
            aniso8601.get_repeating_interval_resolution,
            aniso8601.interval.get_repeating_interval_resolution,
        )
