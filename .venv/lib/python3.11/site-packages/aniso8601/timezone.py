# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

from aniso8601.builders.python import PythonTimeBuilder
from aniso8601.compat import is_string
from aniso8601.exceptions import ISOFormatError


def parse_timezone(tzstr, builder=PythonTimeBuilder):
    # tzstr can be Z, ±hh:mm, ±hhmm, ±hh
    if is_string(tzstr) is False:
        raise ValueError("Time zone must be string.")

    if len(tzstr) == 1 and tzstr[0] == "Z":
        return builder.build_timezone(negative=False, Z=True, name=tzstr)
    elif len(tzstr) == 6:
        # ±hh:mm
        hourstr = tzstr[1:3]
        minutestr = tzstr[4:6]

        if tzstr[0] == "-" and hourstr == "00" and minutestr == "00":
            raise ISOFormatError("Negative ISO 8601 time offset must not " "be 0.")
    elif len(tzstr) == 5:
        # ±hhmm
        hourstr = tzstr[1:3]
        minutestr = tzstr[3:5]

        if tzstr[0] == "-" and hourstr == "00" and minutestr == "00":
            raise ISOFormatError("Negative ISO 8601 time offset must not " "be 0.")
    elif len(tzstr) == 3:
        # ±hh
        hourstr = tzstr[1:3]
        minutestr = None

        if tzstr[0] == "-" and hourstr == "00":
            raise ISOFormatError("Negative ISO 8601 time offset must not " "be 0.")
    else:
        raise ISOFormatError('"{0}" is not a valid ISO 8601 time offset.'.format(tzstr))

    for componentstr in [hourstr, minutestr]:
        if componentstr is not None:
            if componentstr.isdigit() is False:
                raise ISOFormatError(
                    '"{0}" is not a valid ISO 8601 time offset.'.format(tzstr)
                )

    if tzstr[0] == "+":
        return builder.build_timezone(
            negative=False, hh=hourstr, mm=minutestr, name=tzstr
        )

    if tzstr[0] == "-":
        return builder.build_timezone(
            negative=True, hh=hourstr, mm=minutestr, name=tzstr
        )

    raise ISOFormatError('"{0}" is not a valid ISO 8601 time offset.'.format(tzstr))
