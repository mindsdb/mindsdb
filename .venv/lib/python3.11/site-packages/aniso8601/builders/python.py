# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

import datetime
from collections import namedtuple
from functools import partial

from aniso8601.builders import (
    BaseTimeBuilder,
    DatetimeTuple,
    DateTuple,
    Limit,
    TimeTuple,
    TupleBuilder,
    cast,
    range_check,
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

DAYS_PER_YEAR = 365
DAYS_PER_MONTH = 30
DAYS_PER_WEEK = 7

HOURS_PER_DAY = 24

MINUTES_PER_HOUR = 60
MINUTES_PER_DAY = MINUTES_PER_HOUR * HOURS_PER_DAY

SECONDS_PER_MINUTE = 60
SECONDS_PER_DAY = MINUTES_PER_DAY * SECONDS_PER_MINUTE

MICROSECONDS_PER_SECOND = int(1e6)

MICROSECONDS_PER_MINUTE = 60 * MICROSECONDS_PER_SECOND
MICROSECONDS_PER_HOUR = 60 * MICROSECONDS_PER_MINUTE
MICROSECONDS_PER_DAY = 24 * MICROSECONDS_PER_HOUR
MICROSECONDS_PER_WEEK = 7 * MICROSECONDS_PER_DAY
MICROSECONDS_PER_MONTH = DAYS_PER_MONTH * MICROSECONDS_PER_DAY
MICROSECONDS_PER_YEAR = DAYS_PER_YEAR * MICROSECONDS_PER_DAY

TIMEDELTA_MAX_DAYS = datetime.timedelta.max.days

FractionalComponent = namedtuple(
    "FractionalComponent", ["principal", "microsecondremainder"]
)


def year_range_check(valuestr, limit):
    YYYYstr = valuestr

    # Truncated dates, like '19', refer to 1900-1999 inclusive,
    # we simply parse to 1900
    if len(valuestr) < 4:
        # Shift 0s in from the left to form complete year
        YYYYstr = valuestr.ljust(4, "0")

    return range_check(YYYYstr, limit)


def fractional_range_check(conversion, valuestr, limit):
    if valuestr is None:
        return None

    if "." in valuestr:
        castfunc = partial(_cast_to_fractional_component, conversion)
    else:
        castfunc = int

    value = cast(valuestr, castfunc, thrownmessage=limit.casterrorstring)

    if type(value) is FractionalComponent:
        tocheck = float(valuestr)
    else:
        tocheck = int(valuestr)

    if limit.min is not None and tocheck < limit.min:
        raise limit.rangeexception(limit.rangeerrorstring)

    if limit.max is not None and tocheck > limit.max:
        raise limit.rangeexception(limit.rangeerrorstring)

    return value


def _cast_to_fractional_component(conversion, floatstr):
    # Splits a string with a decimal point into an int, and
    # int representing the floating point remainder as a number
    # of microseconds, determined by multiplying by conversion
    intpart, floatpart = floatstr.split(".")

    intvalue = int(intpart)
    preconvertedvalue = int(floatpart)

    convertedvalue = (preconvertedvalue * conversion) // (10 ** len(floatpart))

    return FractionalComponent(intvalue, convertedvalue)


class PythonTimeBuilder(BaseTimeBuilder):
    # 0000 (1 BC) is not representable as a Python date
    DATE_YYYY_LIMIT = Limit(
        "Invalid year string.",
        datetime.MINYEAR,
        datetime.MAXYEAR,
        YearOutOfBoundsError,
        "Year must be between {0}..{1}.".format(datetime.MINYEAR, datetime.MAXYEAR),
        year_range_check,
    )
    TIME_HH_LIMIT = Limit(
        "Invalid hour string.",
        0,
        24,
        HoursOutOfBoundsError,
        "Hour must be between 0..24 with " "24 representing midnight.",
        partial(fractional_range_check, MICROSECONDS_PER_HOUR),
    )
    TIME_MM_LIMIT = Limit(
        "Invalid minute string.",
        0,
        59,
        MinutesOutOfBoundsError,
        "Minute must be between 0..59.",
        partial(fractional_range_check, MICROSECONDS_PER_MINUTE),
    )
    TIME_SS_LIMIT = Limit(
        "Invalid second string.",
        0,
        60,
        SecondsOutOfBoundsError,
        "Second must be between 0..60 with " "60 representing a leap second.",
        partial(fractional_range_check, MICROSECONDS_PER_SECOND),
    )
    DURATION_PNY_LIMIT = Limit(
        "Invalid year duration string.",
        None,
        None,
        YearOutOfBoundsError,
        None,
        partial(fractional_range_check, MICROSECONDS_PER_YEAR),
    )
    DURATION_PNM_LIMIT = Limit(
        "Invalid month duration string.",
        None,
        None,
        MonthOutOfBoundsError,
        None,
        partial(fractional_range_check, MICROSECONDS_PER_MONTH),
    )
    DURATION_PNW_LIMIT = Limit(
        "Invalid week duration string.",
        None,
        None,
        WeekOutOfBoundsError,
        None,
        partial(fractional_range_check, MICROSECONDS_PER_WEEK),
    )
    DURATION_PND_LIMIT = Limit(
        "Invalid day duration string.",
        None,
        None,
        DayOutOfBoundsError,
        None,
        partial(fractional_range_check, MICROSECONDS_PER_DAY),
    )
    DURATION_TNH_LIMIT = Limit(
        "Invalid hour duration string.",
        None,
        None,
        HoursOutOfBoundsError,
        None,
        partial(fractional_range_check, MICROSECONDS_PER_HOUR),
    )
    DURATION_TNM_LIMIT = Limit(
        "Invalid minute duration string.",
        None,
        None,
        MinutesOutOfBoundsError,
        None,
        partial(fractional_range_check, MICROSECONDS_PER_MINUTE),
    )
    DURATION_TNS_LIMIT = Limit(
        "Invalid second duration string.",
        None,
        None,
        SecondsOutOfBoundsError,
        None,
        partial(fractional_range_check, MICROSECONDS_PER_SECOND),
    )

    DATE_RANGE_DICT = BaseTimeBuilder.DATE_RANGE_DICT
    DATE_RANGE_DICT["YYYY"] = DATE_YYYY_LIMIT

    TIME_RANGE_DICT = {"hh": TIME_HH_LIMIT, "mm": TIME_MM_LIMIT, "ss": TIME_SS_LIMIT}

    DURATION_RANGE_DICT = {
        "PnY": DURATION_PNY_LIMIT,
        "PnM": DURATION_PNM_LIMIT,
        "PnW": DURATION_PNW_LIMIT,
        "PnD": DURATION_PND_LIMIT,
        "TnH": DURATION_TNH_LIMIT,
        "TnM": DURATION_TNM_LIMIT,
        "TnS": DURATION_TNS_LIMIT,
    }

    @classmethod
    def build_date(cls, YYYY=None, MM=None, DD=None, Www=None, D=None, DDD=None):
        YYYY, MM, DD, Www, D, DDD = cls.range_check_date(YYYY, MM, DD, Www, D, DDD)

        if MM is None:
            MM = 1

        if DD is None:
            DD = 1

        if DDD is not None:
            return PythonTimeBuilder._build_ordinal_date(YYYY, DDD)

        if Www is not None:
            return PythonTimeBuilder._build_week_date(YYYY, Www, isoday=D)

        return datetime.date(YYYY, MM, DD)

    @classmethod
    def build_time(cls, hh=None, mm=None, ss=None, tz=None):
        # Builds a time from the given parts, handling fractional arguments
        # where necessary
        hours = 0
        minutes = 0
        seconds = 0
        microseconds = 0

        hh, mm, ss, tz = cls.range_check_time(hh, mm, ss, tz)

        if type(hh) is FractionalComponent:
            hours = hh.principal
            microseconds = hh.microsecondremainder
        elif hh is not None:
            hours = hh

        if type(mm) is FractionalComponent:
            minutes = mm.principal
            microseconds = mm.microsecondremainder
        elif mm is not None:
            minutes = mm

        if type(ss) is FractionalComponent:
            seconds = ss.principal
            microseconds = ss.microsecondremainder
        elif ss is not None:
            seconds = ss

        (
            hours,
            minutes,
            seconds,
            microseconds,
        ) = PythonTimeBuilder._distribute_microseconds(
            microseconds,
            (hours, minutes, seconds),
            (MICROSECONDS_PER_HOUR, MICROSECONDS_PER_MINUTE, MICROSECONDS_PER_SECOND),
        )

        # Move midnight into range
        if hours == 24:
            hours = 0

        # Datetimes don't handle fractional components, so we use a timedelta
        if tz is not None:
            return (
                datetime.datetime(
                    1, 1, 1, hour=hours, minute=minutes, tzinfo=cls._build_object(tz)
                )
                + datetime.timedelta(seconds=seconds, microseconds=microseconds)
            ).timetz()

        return (
            datetime.datetime(1, 1, 1, hour=hours, minute=minutes)
            + datetime.timedelta(seconds=seconds, microseconds=microseconds)
        ).time()

    @classmethod
    def build_datetime(cls, date, time):
        return datetime.datetime.combine(
            cls._build_object(date), cls._build_object(time)
        )

    @classmethod
    def build_duration(
        cls, PnY=None, PnM=None, PnW=None, PnD=None, TnH=None, TnM=None, TnS=None
    ):
        # PnY and PnM will be distributed to PnD, microsecond remainder to TnS
        PnY, PnM, PnW, PnD, TnH, TnM, TnS = cls.range_check_duration(
            PnY, PnM, PnW, PnD, TnH, TnM, TnS
        )

        seconds = TnS.principal
        microseconds = TnS.microsecondremainder

        return datetime.timedelta(
            days=PnD,
            seconds=seconds,
            microseconds=microseconds,
            minutes=TnM,
            hours=TnH,
            weeks=PnW,
        )

    @classmethod
    def build_interval(cls, start=None, end=None, duration=None):
        start, end, duration = cls.range_check_interval(start, end, duration)

        if start is not None and end is not None:
            # <start>/<end>
            startobject = cls._build_object(start)
            endobject = cls._build_object(end)

            return (startobject, endobject)

        durationobject = cls._build_object(duration)

        # Determine if datetime promotion is required
        datetimerequired = (
            duration.TnH is not None
            or duration.TnM is not None
            or duration.TnS is not None
            or durationobject.seconds != 0
            or durationobject.microseconds != 0
        )

        if end is not None:
            # <duration>/<end>
            endobject = cls._build_object(end)

            # Range check
            if type(end) is DateTuple and datetimerequired is True:
                # <end> is a date, and <duration> requires datetime resolution
                return (
                    endobject,
                    cls.build_datetime(end, TupleBuilder.build_time()) - durationobject,
                )

            return (endobject, endobject - durationobject)

        # <start>/<duration>
        startobject = cls._build_object(start)

        # Range check
        if type(start) is DateTuple and datetimerequired is True:
            # <start> is a date, and <duration> requires datetime resolution
            return (
                startobject,
                cls.build_datetime(start, TupleBuilder.build_time()) + durationobject,
            )

        return (startobject, startobject + durationobject)

    @classmethod
    def build_repeating_interval(cls, R=None, Rnn=None, interval=None):
        startobject = None
        endobject = None

        R, Rnn, interval = cls.range_check_repeating_interval(R, Rnn, interval)

        if interval.start is not None:
            startobject = cls._build_object(interval.start)

        if interval.end is not None:
            endobject = cls._build_object(interval.end)

        if interval.duration is not None:
            durationobject = cls._build_object(interval.duration)
        else:
            durationobject = endobject - startobject

        if R is True:
            if startobject is not None:
                return cls._date_generator_unbounded(startobject, durationobject)

            return cls._date_generator_unbounded(endobject, -durationobject)

        iterations = int(Rnn)

        if startobject is not None:
            return cls._date_generator(startobject, durationobject, iterations)

        return cls._date_generator(endobject, -durationobject, iterations)

    @classmethod
    def build_timezone(cls, negative=None, Z=None, hh=None, mm=None, name=""):
        negative, Z, hh, mm, name = cls.range_check_timezone(negative, Z, hh, mm, name)

        if Z is True:
            # Z -> UTC
            return UTCOffset(name="UTC", minutes=0)

        tzhour = int(hh)

        if mm is not None:
            tzminute = int(mm)
        else:
            tzminute = 0

        if negative is True:
            return UTCOffset(name=name, minutes=-(tzhour * 60 + tzminute))

        return UTCOffset(name=name, minutes=tzhour * 60 + tzminute)

    @classmethod
    def range_check_duration(
        cls,
        PnY=None,
        PnM=None,
        PnW=None,
        PnD=None,
        TnH=None,
        TnM=None,
        TnS=None,
        rangedict=None,
    ):
        years = 0
        months = 0
        days = 0
        weeks = 0
        hours = 0
        minutes = 0
        seconds = 0
        microseconds = 0

        PnY, PnM, PnW, PnD, TnH, TnM, TnS = BaseTimeBuilder.range_check_duration(
            PnY, PnM, PnW, PnD, TnH, TnM, TnS, rangedict=cls.DURATION_RANGE_DICT
        )

        if PnY is not None:
            if type(PnY) is FractionalComponent:
                years = PnY.principal
                microseconds = PnY.microsecondremainder
            else:
                years = PnY

            if years * DAYS_PER_YEAR > TIMEDELTA_MAX_DAYS:
                raise YearOutOfBoundsError("Duration exceeds maximum timedelta size.")

        if PnM is not None:
            if type(PnM) is FractionalComponent:
                months = PnM.principal
                microseconds = PnM.microsecondremainder
            else:
                months = PnM

            if months * DAYS_PER_MONTH > TIMEDELTA_MAX_DAYS:
                raise MonthOutOfBoundsError("Duration exceeds maximum timedelta size.")

        if PnW is not None:
            if type(PnW) is FractionalComponent:
                weeks = PnW.principal
                microseconds = PnW.microsecondremainder
            else:
                weeks = PnW

            if weeks * DAYS_PER_WEEK > TIMEDELTA_MAX_DAYS:
                raise WeekOutOfBoundsError("Duration exceeds maximum timedelta size.")

        if PnD is not None:
            if type(PnD) is FractionalComponent:
                days = PnD.principal
                microseconds = PnD.microsecondremainder
            else:
                days = PnD

            if days > TIMEDELTA_MAX_DAYS:
                raise DayOutOfBoundsError("Duration exceeds maximum timedelta size.")

        if TnH is not None:
            if type(TnH) is FractionalComponent:
                hours = TnH.principal
                microseconds = TnH.microsecondremainder
            else:
                hours = TnH

            if hours // HOURS_PER_DAY > TIMEDELTA_MAX_DAYS:
                raise HoursOutOfBoundsError("Duration exceeds maximum timedelta size.")

        if TnM is not None:
            if type(TnM) is FractionalComponent:
                minutes = TnM.principal
                microseconds = TnM.microsecondremainder
            else:
                minutes = TnM

            if minutes // MINUTES_PER_DAY > TIMEDELTA_MAX_DAYS:
                raise MinutesOutOfBoundsError(
                    "Duration exceeds maximum timedelta size."
                )

        if TnS is not None:
            if type(TnS) is FractionalComponent:
                seconds = TnS.principal
                microseconds = TnS.microsecondremainder
            else:
                seconds = TnS

            if seconds // SECONDS_PER_DAY > TIMEDELTA_MAX_DAYS:
                raise SecondsOutOfBoundsError(
                    "Duration exceeds maximum timedelta size."
                )

        (
            years,
            months,
            weeks,
            days,
            hours,
            minutes,
            seconds,
            microseconds,
        ) = PythonTimeBuilder._distribute_microseconds(
            microseconds,
            (years, months, weeks, days, hours, minutes, seconds),
            (
                MICROSECONDS_PER_YEAR,
                MICROSECONDS_PER_MONTH,
                MICROSECONDS_PER_WEEK,
                MICROSECONDS_PER_DAY,
                MICROSECONDS_PER_HOUR,
                MICROSECONDS_PER_MINUTE,
                MICROSECONDS_PER_SECOND,
            ),
        )

        # Note that weeks can be handled without conversion to days
        totaldays = years * DAYS_PER_YEAR + months * DAYS_PER_MONTH + days

        # Check against timedelta limits
        if (
            totaldays
            + weeks * DAYS_PER_WEEK
            + hours // HOURS_PER_DAY
            + minutes // MINUTES_PER_DAY
            + seconds // SECONDS_PER_DAY
            > TIMEDELTA_MAX_DAYS
        ):
            raise DayOutOfBoundsError("Duration exceeds maximum timedelta size.")

        return (
            None,
            None,
            weeks,
            totaldays,
            hours,
            minutes,
            FractionalComponent(seconds, microseconds),
        )

    @classmethod
    def range_check_interval(cls, start=None, end=None, duration=None):
        # Handles concise format, range checks any potential durations
        if start is not None and end is not None:
            # <start>/<end>
            # Handle concise format
            if cls._is_interval_end_concise(end) is True:
                end = cls._combine_concise_interval_tuples(start, end)

            return (start, end, duration)

        durationobject = cls._build_object(duration)

        if end is not None:
            # <duration>/<end>
            endobject = cls._build_object(end)

            # Range check
            if type(end) is DateTuple:
                enddatetime = cls.build_datetime(end, TupleBuilder.build_time())

                if enddatetime - datetime.datetime.min < durationobject:
                    raise YearOutOfBoundsError("Interval end less than minimium date.")
            else:
                mindatetime = datetime.datetime.min

                if end.time.tz is not None:
                    mindatetime = mindatetime.replace(tzinfo=endobject.tzinfo)

                if endobject - mindatetime < durationobject:
                    raise YearOutOfBoundsError("Interval end less than minimium date.")
        else:
            # <start>/<duration>
            startobject = cls._build_object(start)

            # Range check
            if type(start) is DateTuple:
                startdatetime = cls.build_datetime(start, TupleBuilder.build_time())

                if datetime.datetime.max - startdatetime < durationobject:
                    raise YearOutOfBoundsError(
                        "Interval end greater than maximum date."
                    )
            else:
                maxdatetime = datetime.datetime.max

                if start.time.tz is not None:
                    maxdatetime = maxdatetime.replace(tzinfo=startobject.tzinfo)

                if maxdatetime - startobject < durationobject:
                    raise YearOutOfBoundsError(
                        "Interval end greater than maximum date."
                    )

        return (start, end, duration)

    @staticmethod
    def _build_week_date(isoyear, isoweek, isoday=None):
        if isoday is None:
            return PythonTimeBuilder._iso_year_start(isoyear) + datetime.timedelta(
                weeks=isoweek - 1
            )

        return PythonTimeBuilder._iso_year_start(isoyear) + datetime.timedelta(
            weeks=isoweek - 1, days=isoday - 1
        )

    @staticmethod
    def _build_ordinal_date(isoyear, isoday):
        # Day of year to a date
        # https://stackoverflow.com/questions/2427555/python-question-year-and-day-of-year-to-date
        builtdate = datetime.date(isoyear, 1, 1) + datetime.timedelta(days=isoday - 1)

        return builtdate

    @staticmethod
    def _iso_year_start(isoyear):
        # Given an ISO year, returns the equivalent of the start of the year
        # on the Gregorian calendar (which is used by Python)
        # Stolen from:
        # http://stackoverflow.com/questions/304256/whats-the-best-way-to-find-the-inverse-of-datetime-isocalendar

        # Determine the location of the 4th of January, the first week of
        # the ISO year is the week containing the 4th of January
        # http://en.wikipedia.org/wiki/ISO_week_date
        fourth_jan = datetime.date(isoyear, 1, 4)

        # Note the conversion from ISO day (1 - 7) and Python day (0 - 6)
        delta = datetime.timedelta(days=fourth_jan.isoweekday() - 1)

        # Return the start of the year
        return fourth_jan - delta

    @staticmethod
    def _date_generator(startdate, timedelta, iterations):
        currentdate = startdate
        currentiteration = 0

        while currentiteration < iterations:
            yield currentdate

            # Update the values
            currentdate += timedelta
            currentiteration += 1

    @staticmethod
    def _date_generator_unbounded(startdate, timedelta):
        currentdate = startdate

        while True:
            yield currentdate

            # Update the value
            currentdate += timedelta

    @staticmethod
    def _distribute_microseconds(todistribute, recipients, reductions):
        # Given a number of microseconds as int, a tuple of ints length n
        # to distribute to, and a tuple of ints length n to divide todistribute
        # by (from largest to smallest), returns a tuple of length n + 1, with
        # todistribute divided across recipients using the reductions, with
        # the final remainder returned as the final tuple member
        results = []

        remainder = todistribute

        for index, reduction in enumerate(reductions):
            additional, remainder = divmod(remainder, reduction)

            results.append(recipients[index] + additional)

        # Always return the remaining microseconds
        results.append(remainder)

        return tuple(results)
