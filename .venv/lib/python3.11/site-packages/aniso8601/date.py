# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

from aniso8601.builders import TupleBuilder
from aniso8601.builders.python import PythonTimeBuilder
from aniso8601.compat import is_string
from aniso8601.exceptions import ISOFormatError
from aniso8601.resolution import DateResolution


def get_date_resolution(isodatestr):
    # Valid string formats are:
    #
    # Y[YYY]
    # YYYY-MM-DD
    # YYYYMMDD
    # YYYY-MM
    # YYYY-Www
    # YYYYWww
    # YYYY-Www-D
    # YYYYWwwD
    # YYYY-DDD
    # YYYYDDD
    isodatetuple = parse_date(isodatestr, builder=TupleBuilder)

    if isodatetuple.DDD is not None:
        # YYYY-DDD
        # YYYYDDD
        return DateResolution.Ordinal

    if isodatetuple.D is not None:
        # YYYY-Www-D
        # YYYYWwwD
        return DateResolution.Weekday

    if isodatetuple.Www is not None:
        # YYYY-Www
        # YYYYWww
        return DateResolution.Week

    if isodatetuple.DD is not None:
        # YYYY-MM-DD
        # YYYYMMDD
        return DateResolution.Day

    if isodatetuple.MM is not None:
        # YYYY-MM
        return DateResolution.Month

    # Y[YYY]
    return DateResolution.Year


def parse_date(isodatestr, builder=PythonTimeBuilder):
    # Given a string in any ISO 8601 date format, return a datetime.date
    # object that corresponds to the given date. Valid string formats are:
    #
    # Y[YYY]
    # YYYY-MM-DD
    # YYYYMMDD
    # YYYY-MM
    # YYYY-Www
    # YYYYWww
    # YYYY-Www-D
    # YYYYWwwD
    # YYYY-DDD
    # YYYYDDD
    if is_string(isodatestr) is False:
        raise ValueError("Date must be string.")

    if isodatestr.startswith("+") or isodatestr.startswith("-"):
        raise NotImplementedError(
            "ISO 8601 extended year representation " "not supported."
        )

    if len(isodatestr) == 0 or isodatestr.count("-") > 2:
        raise ISOFormatError('"{0}" is not a valid ISO 8601 date.'.format(isodatestr))
    yearstr = None
    monthstr = None
    daystr = None
    weekstr = None
    weekdaystr = None
    ordinaldaystr = None

    if len(isodatestr) <= 4:
        # Y[YYY]
        yearstr = isodatestr
    elif "W" in isodatestr:
        if len(isodatestr) == 10:
            # YYYY-Www-D
            yearstr = isodatestr[0:4]
            weekstr = isodatestr[6:8]
            weekdaystr = isodatestr[9]
        elif len(isodatestr) == 8:
            if "-" in isodatestr:
                # YYYY-Www
                yearstr = isodatestr[0:4]
                weekstr = isodatestr[6:]
            else:
                # YYYYWwwD
                yearstr = isodatestr[0:4]
                weekstr = isodatestr[5:7]
                weekdaystr = isodatestr[7]
        elif len(isodatestr) == 7:
            # YYYYWww
            yearstr = isodatestr[0:4]
            weekstr = isodatestr[5:]
    elif len(isodatestr) == 7:
        if "-" in isodatestr:
            # YYYY-MM
            yearstr = isodatestr[0:4]
            monthstr = isodatestr[5:]
        else:
            # YYYYDDD
            yearstr = isodatestr[0:4]
            ordinaldaystr = isodatestr[4:]
    elif len(isodatestr) == 8:
        if "-" in isodatestr:
            # YYYY-DDD
            yearstr = isodatestr[0:4]
            ordinaldaystr = isodatestr[5:]
        else:
            # YYYYMMDD
            yearstr = isodatestr[0:4]
            monthstr = isodatestr[4:6]
            daystr = isodatestr[6:]
    elif len(isodatestr) == 10:
        # YYYY-MM-DD
        yearstr = isodatestr[0:4]
        monthstr = isodatestr[5:7]
        daystr = isodatestr[8:]
    else:
        raise ISOFormatError('"{0}" is not a valid ISO 8601 date.'.format(isodatestr))

    hascomponent = False

    for componentstr in [yearstr, monthstr, daystr, weekstr, weekdaystr, ordinaldaystr]:
        if componentstr is not None:
            hascomponent = True

            if componentstr.isdigit() is False:
                raise ISOFormatError(
                    '"{0}" is not a valid ISO 8601 date.'.format(isodatestr)
                )

    if hascomponent is False:
        raise ISOFormatError('"{0}" is not a valid ISO 8601 date.'.format(isodatestr))

    return builder.build_date(
        YYYY=yearstr,
        MM=monthstr,
        DD=daystr,
        Www=weekstr,
        D=weekdaystr,
        DDD=ordinaldaystr,
    )
