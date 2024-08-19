# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

from aniso8601.builders import DatetimeTuple, DateTuple, TupleBuilder
from aniso8601.builders.python import PythonTimeBuilder
from aniso8601.compat import is_string
from aniso8601.date import parse_date
from aniso8601.duration import parse_duration
from aniso8601.exceptions import ISOFormatError
from aniso8601.resolution import IntervalResolution
from aniso8601.time import parse_datetime, parse_time


def get_interval_resolution(
    isointervalstr, intervaldelimiter="/", datetimedelimiter="T"
):
    isointervaltuple = parse_interval(
        isointervalstr,
        intervaldelimiter=intervaldelimiter,
        datetimedelimiter=datetimedelimiter,
        builder=TupleBuilder,
    )

    return _get_interval_resolution(isointervaltuple)


def get_repeating_interval_resolution(
    isointervalstr, intervaldelimiter="/", datetimedelimiter="T"
):
    repeatingintervaltuple = parse_repeating_interval(
        isointervalstr,
        intervaldelimiter=intervaldelimiter,
        datetimedelimiter=datetimedelimiter,
        builder=TupleBuilder,
    )

    return _get_interval_resolution(repeatingintervaltuple.interval)


def _get_interval_resolution(intervaltuple):
    if intervaltuple.start is not None and intervaltuple.end is not None:
        return max(
            _get_interval_component_resolution(intervaltuple.start),
            _get_interval_component_resolution(intervaltuple.end),
        )

    if intervaltuple.start is not None and intervaltuple.duration is not None:
        return max(
            _get_interval_component_resolution(intervaltuple.start),
            _get_interval_component_resolution(intervaltuple.duration),
        )

    return max(
        _get_interval_component_resolution(intervaltuple.end),
        _get_interval_component_resolution(intervaltuple.duration),
    )


def _get_interval_component_resolution(componenttuple):
    if type(componenttuple) is DateTuple:
        if componenttuple.DDD is not None:
            # YYYY-DDD
            # YYYYDDD
            return IntervalResolution.Ordinal

        if componenttuple.D is not None:
            # YYYY-Www-D
            # YYYYWwwD
            return IntervalResolution.Weekday

        if componenttuple.Www is not None:
            # YYYY-Www
            # YYYYWww
            return IntervalResolution.Week

        if componenttuple.DD is not None:
            # YYYY-MM-DD
            # YYYYMMDD
            return IntervalResolution.Day

        if componenttuple.MM is not None:
            # YYYY-MM
            return IntervalResolution.Month

        # Y[YYY]
        return IntervalResolution.Year
    elif type(componenttuple) is DatetimeTuple:
        # Datetime
        if componenttuple.time.ss is not None:
            return IntervalResolution.Seconds

        if componenttuple.time.mm is not None:
            return IntervalResolution.Minutes

        return IntervalResolution.Hours

    # Duration
    if componenttuple.TnS is not None:
        return IntervalResolution.Seconds

    if componenttuple.TnM is not None:
        return IntervalResolution.Minutes

    if componenttuple.TnH is not None:
        return IntervalResolution.Hours

    if componenttuple.PnD is not None:
        return IntervalResolution.Day

    if componenttuple.PnW is not None:
        return IntervalResolution.Week

    if componenttuple.PnM is not None:
        return IntervalResolution.Month

    return IntervalResolution.Year


def parse_interval(
    isointervalstr,
    intervaldelimiter="/",
    datetimedelimiter="T",
    builder=PythonTimeBuilder,
):
    # Given a string representing an ISO 8601 interval, return an
    # interval built by the given builder. Valid formats are:
    #
    # <start>/<end>
    # <start>/<duration>
    # <duration>/<end>
    #
    # The <start> and <end> values can represent dates, or datetimes,
    # not times.
    #
    # The format:
    #
    # <duration>
    #
    # Is expressly not supported as there is no way to provide the additional
    # required context.

    if is_string(isointervalstr) is False:
        raise ValueError("Interval must be string.")

    if len(isointervalstr) == 0:
        raise ISOFormatError("Interval string is empty.")

    if isointervalstr[0] == "R":
        raise ISOFormatError(
            "ISO 8601 repeating intervals must be parsed "
            "with parse_repeating_interval."
        )

    intervaldelimitercount = isointervalstr.count(intervaldelimiter)

    if intervaldelimitercount == 0:
        raise ISOFormatError(
            'Interval delimiter "{0}" is not in interval '
            'string "{1}".'.format(intervaldelimiter, isointervalstr)
        )

    if intervaldelimitercount > 1:
        raise ISOFormatError(
            "{0} is not a valid ISO 8601 interval".format(isointervalstr)
        )

    return _parse_interval(
        isointervalstr, builder, intervaldelimiter, datetimedelimiter
    )


def parse_repeating_interval(
    isointervalstr,
    intervaldelimiter="/",
    datetimedelimiter="T",
    builder=PythonTimeBuilder,
):
    # Given a string representing an ISO 8601 interval repeating, return an
    # interval built by the given builder. Valid formats are:
    #
    # Rnn/<interval>
    # R/<interval>

    if not isinstance(isointervalstr, str):
        raise ValueError("Interval must be string.")

    if len(isointervalstr) == 0:
        raise ISOFormatError("Repeating interval string is empty.")

    if isointervalstr[0] != "R":
        raise ISOFormatError("ISO 8601 repeating interval must start " "with an R.")

    if intervaldelimiter not in isointervalstr:
        raise ISOFormatError(
            'Interval delimiter "{0}" is not in interval '
            'string "{1}".'.format(intervaldelimiter, isointervalstr)
        )

    # Parse the number of iterations
    iterationpart, intervalpart = isointervalstr.split(intervaldelimiter, 1)

    if len(iterationpart) > 1:
        R = False
        Rnn = iterationpart[1:]
    else:
        R = True
        Rnn = None

    interval = _parse_interval(
        intervalpart, TupleBuilder, intervaldelimiter, datetimedelimiter
    )

    return builder.build_repeating_interval(R=R, Rnn=Rnn, interval=interval)


def _parse_interval(
    isointervalstr, builder, intervaldelimiter="/", datetimedelimiter="T"
):
    # Returns a tuple containing the start of the interval, the end of the
    # interval, and or the interval duration

    firstpart, secondpart = isointervalstr.split(intervaldelimiter)

    if len(firstpart) == 0 or len(secondpart) == 0:
        raise ISOFormatError(
            "{0} is not a valid ISO 8601 interval".format(isointervalstr)
        )

    if firstpart[0] == "P":
        # <duration>/<end>
        # Notice that these are not returned 'in order' (earlier to later), this
        # is to maintain consistency with parsing <start>/<end> durations, as
        # well as making repeating interval code cleaner. Users who desire
        # durations to be in order can use the 'sorted' operator.
        duration = parse_duration(firstpart, builder=TupleBuilder)

        # We need to figure out if <end> is a date, or a datetime
        if secondpart.find(datetimedelimiter) != -1:
            # <end> is a datetime
            endtuple = parse_datetime(
                secondpart, delimiter=datetimedelimiter, builder=TupleBuilder
            )
        else:
            endtuple = parse_date(secondpart, builder=TupleBuilder)

        return builder.build_interval(end=endtuple, duration=duration)
    elif secondpart[0] == "P":
        # <start>/<duration>
        # We need to figure out if <start> is a date, or a datetime
        duration = parse_duration(secondpart, builder=TupleBuilder)

        if firstpart.find(datetimedelimiter) != -1:
            # <start> is a datetime
            starttuple = parse_datetime(
                firstpart, delimiter=datetimedelimiter, builder=TupleBuilder
            )
        else:
            # <start> must just be a date
            starttuple = parse_date(firstpart, builder=TupleBuilder)

        return builder.build_interval(start=starttuple, duration=duration)

    # <start>/<end>
    if firstpart.find(datetimedelimiter) != -1:
        # Both parts are datetimes
        starttuple = parse_datetime(
            firstpart, delimiter=datetimedelimiter, builder=TupleBuilder
        )
    else:
        starttuple = parse_date(firstpart, builder=TupleBuilder)

    endtuple = _parse_interval_end(secondpart, starttuple, datetimedelimiter)

    return builder.build_interval(start=starttuple, end=endtuple)


def _parse_interval_end(endstr, starttuple, datetimedelimiter):
    datestr = None
    timestr = None

    monthstr = None
    daystr = None

    concise = False

    if type(starttuple) is DateTuple:
        startdatetuple = starttuple
    else:
        # Start is a datetime
        startdatetuple = starttuple.date

    if datetimedelimiter in endstr:
        datestr, timestr = endstr.split(datetimedelimiter, 1)
    elif ":" in endstr:
        timestr = endstr
    else:
        datestr = endstr

    if timestr is not None:
        endtimetuple = parse_time(timestr, builder=TupleBuilder)

    # End is just a time
    if datestr is None:
        return endtimetuple

    # Handle backwards concise representation
    if datestr.count("-") == 1:
        monthstr, daystr = datestr.split("-")
        concise = True
    elif len(datestr) <= 2:
        daystr = datestr
        concise = True
    elif len(datestr) <= 4:
        monthstr = datestr[0:2]
        daystr = datestr[2:]
        concise = True

    if concise is True:
        concisedatestr = startdatetuple.YYYY

        # Separators required because concise elements may be missing digits
        if monthstr is not None:
            concisedatestr += "-" + monthstr
        elif startdatetuple.MM is not None:
            concisedatestr += "-" + startdatetuple.MM

        concisedatestr += "-" + daystr

        enddatetuple = parse_date(concisedatestr, builder=TupleBuilder)

        # Clear unsupplied components
        if monthstr is None:
            enddatetuple = TupleBuilder.build_date(DD=enddatetuple.DD)
        else:
            # Year not provided
            enddatetuple = TupleBuilder.build_date(
                MM=enddatetuple.MM, DD=enddatetuple.DD
            )
    else:
        enddatetuple = parse_date(datestr, builder=TupleBuilder)

    if timestr is None:
        return enddatetuple

    return TupleBuilder.build_datetime(enddatetuple, endtimetuple)
