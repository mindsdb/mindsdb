# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

from aniso8601.builders import TupleBuilder
from aniso8601.builders.python import PythonTimeBuilder
from aniso8601.compat import is_string
from aniso8601.date import parse_date
from aniso8601.decimalfraction import normalize
from aniso8601.exceptions import ISOFormatError
from aniso8601.resolution import TimeResolution
from aniso8601.timezone import parse_timezone

TIMEZONE_DELIMITERS = ["Z", "+", "-"]


def get_time_resolution(isotimestr):
    # Valid time formats are:
    #
    # hh:mm:ss
    # hhmmss
    # hh:mm
    # hhmm
    # hh
    # hh:mm:ssZ
    # hhmmssZ
    # hh:mmZ
    # hhmmZ
    # hhZ
    # hh:mm:ss±hh:mm
    # hhmmss±hh:mm
    # hh:mm±hh:mm
    # hhmm±hh:mm
    # hh±hh:mm
    # hh:mm:ss±hhmm
    # hhmmss±hhmm
    # hh:mm±hhmm
    # hhmm±hhmm
    # hh±hhmm
    # hh:mm:ss±hh
    # hhmmss±hh
    # hh:mm±hh
    # hhmm±hh
    # hh±hh
    isotimetuple = parse_time(isotimestr, builder=TupleBuilder)

    return _get_time_resolution(isotimetuple)


def get_datetime_resolution(isodatetimestr, delimiter="T"):
    # <date>T<time>
    #
    # Time part cannot be omittted so return time resolution
    isotimetuple = parse_datetime(
        isodatetimestr, delimiter=delimiter, builder=TupleBuilder
    ).time

    return _get_time_resolution(isotimetuple)


def _get_time_resolution(isotimetuple):
    if isotimetuple.ss is not None:
        return TimeResolution.Seconds

    if isotimetuple.mm is not None:
        return TimeResolution.Minutes

    return TimeResolution.Hours


def parse_time(isotimestr, builder=PythonTimeBuilder):
    # Given a string in any ISO 8601 time format, return a datetime.time object
    # that corresponds to the given time. Fixed offset tzdata will be included
    # if UTC offset is given in the input string. Valid time formats are:
    #
    # hh:mm:ss
    # hhmmss
    # hh:mm
    # hhmm
    # hh
    # hh:mm:ssZ
    # hhmmssZ
    # hh:mmZ
    # hhmmZ
    # hhZ
    # hh:mm:ss±hh:mm
    # hhmmss±hh:mm
    # hh:mm±hh:mm
    # hhmm±hh:mm
    # hh±hh:mm
    # hh:mm:ss±hhmm
    # hhmmss±hhmm
    # hh:mm±hhmm
    # hhmm±hhmm
    # hh±hhmm
    # hh:mm:ss±hh
    # hhmmss±hh
    # hh:mm±hh
    # hhmm±hh
    # hh±hh
    if is_string(isotimestr) is False:
        raise ValueError("Time must be string.")

    if len(isotimestr) == 0:
        raise ISOFormatError('"{0}" is not a valid ISO 8601 time.'.format(isotimestr))

    timestr = normalize(isotimestr)

    hourstr = None
    minutestr = None
    secondstr = None
    tzstr = None

    fractionalstr = None

    # Split out the timezone
    for delimiter in TIMEZONE_DELIMITERS:
        delimiteridx = timestr.find(delimiter)

        if delimiteridx != -1:
            tzstr = timestr[delimiteridx:]
            timestr = timestr[0:delimiteridx]

    # Split out the fractional component
    if timestr.find(".") != -1:
        timestr, fractionalstr = timestr.split(".", 1)

        if fractionalstr.isdigit() is False:
            raise ISOFormatError(
                '"{0}" is not a valid ISO 8601 time.'.format(isotimestr)
            )

    if len(timestr) == 2:
        # hh
        hourstr = timestr
    elif len(timestr) == 4 or len(timestr) == 5:
        # hh:mm
        # hhmm
        if timestr.count(":") == 1:
            hourstr, minutestr = timestr.split(":")
        else:
            hourstr = timestr[0:2]
            minutestr = timestr[2:]
    elif len(timestr) == 6 or len(timestr) == 8:
        # hh:mm:ss
        # hhmmss
        if timestr.count(":") == 2:
            hourstr, minutestr, secondstr = timestr.split(":")
        else:
            hourstr = timestr[0:2]
            minutestr = timestr[2:4]
            secondstr = timestr[4:]
    else:
        raise ISOFormatError('"{0}" is not a valid ISO 8601 time.'.format(isotimestr))

    for componentstr in [hourstr, minutestr, secondstr]:
        if componentstr is not None and componentstr.isdigit() is False:
            raise ISOFormatError(
                '"{0}" is not a valid ISO 8601 time.'.format(isotimestr)
            )

    if fractionalstr is not None:
        if secondstr is not None:
            secondstr = secondstr + "." + fractionalstr
        elif minutestr is not None:
            minutestr = minutestr + "." + fractionalstr
        else:
            hourstr = hourstr + "." + fractionalstr

    if tzstr is None:
        tz = None
    else:
        tz = parse_timezone(tzstr, builder=TupleBuilder)

    return builder.build_time(hh=hourstr, mm=minutestr, ss=secondstr, tz=tz)


def parse_datetime(isodatetimestr, delimiter="T", builder=PythonTimeBuilder):
    # Given a string in ISO 8601 date time format, return a datetime.datetime
    # object that corresponds to the given date time.
    # By default, the ISO 8601 specified T delimiter is used to split the
    # date and time (<date>T<time>). Fixed offset tzdata will be included
    # if UTC offset is given in the input string.
    if is_string(isodatetimestr) is False:
        raise ValueError("Date time must be string.")

    if delimiter not in isodatetimestr:
        raise ISOFormatError(
            'Delimiter "{0}" is not in combined date time '
            'string "{1}".'.format(delimiter, isodatetimestr)
        )

    isodatestr, isotimestr = isodatetimestr.split(delimiter, 1)

    datepart = parse_date(isodatestr, builder=TupleBuilder)

    timepart = parse_time(isotimestr, builder=TupleBuilder)

    return builder.build_datetime(datepart, timepart)
