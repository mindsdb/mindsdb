# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

import datetime


class UTCOffset(datetime.tzinfo):
    def __init__(self, name=None, minutes=None):
        # We build an offset in this manner since the
        # tzinfo class must have an init
        # "method that can be called with no arguments"
        self._name = name

        if minutes is not None:
            self._utcdelta = datetime.timedelta(minutes=minutes)
        else:
            self._utcdelta = None

    def __repr__(self):
        if self._utcdelta >= datetime.timedelta(hours=0):
            return "+{0} UTC".format(self._utcdelta)

        # From the docs:
        # String representations of timedelta objects are normalized
        # similarly to their internal representation. This leads to
        # somewhat unusual results for negative timedeltas.

        # Clean this up for printing purposes
        # Negative deltas start at -1 day
        correcteddays = abs(self._utcdelta.days + 1)

        # Negative deltas have a positive seconds
        deltaseconds = (24 * 60 * 60) - self._utcdelta.seconds

        # (24 hours / day) * (60 minutes / hour) * (60 seconds / hour)
        days, remainder = divmod(deltaseconds, 24 * 60 * 60)

        # (1 hour) * (60 minutes / hour) * (60 seconds / hour)
        hours, remainder = divmod(remainder, 1 * 60 * 60)

        # (1 minute) * (60 seconds / minute)
        minutes, seconds = divmod(remainder, 1 * 60)

        # Add any remaining days to the correcteddays count
        correcteddays += days

        if correcteddays == 0:
            return "-{0}:{1:02}:{2:02} UTC".format(hours, minutes, seconds)
        elif correcteddays == 1:
            return "-1 day, {0}:{1:02}:{2:02} UTC".format(hours, minutes, seconds)

        return "-{0} days, {1}:{2:02}:{3:02} UTC".format(
            correcteddays, hours, minutes, seconds
        )

    def utcoffset(self, dt):
        return self._utcdelta

    def tzname(self, dt):
        return self._name

    def dst(self, dt):
        # ISO 8601 specifies offsets should be different if DST is required,
        # instead of allowing for a DST to be specified
        # https://docs.python.org/2/library/datetime.html#datetime.tzinfo.dst
        return datetime.timedelta(0)
