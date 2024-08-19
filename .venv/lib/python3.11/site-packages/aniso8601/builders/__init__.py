# -*- coding: utf-8 -*-

# Copyright (c) 2021, Brandon Nielsen
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license.  See the LICENSE file for details.

import calendar
from collections import namedtuple

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

DateTuple = namedtuple("Date", ["YYYY", "MM", "DD", "Www", "D", "DDD"])
TimeTuple = namedtuple("Time", ["hh", "mm", "ss", "tz"])
DatetimeTuple = namedtuple("Datetime", ["date", "time"])
DurationTuple = namedtuple(
    "Duration", ["PnY", "PnM", "PnW", "PnD", "TnH", "TnM", "TnS"]
)
IntervalTuple = namedtuple("Interval", ["start", "end", "duration"])
RepeatingIntervalTuple = namedtuple("RepeatingInterval", ["R", "Rnn", "interval"])
TimezoneTuple = namedtuple("Timezone", ["negative", "Z", "hh", "mm", "name"])

Limit = namedtuple(
    "Limit",
    [
        "casterrorstring",
        "min",
        "max",
        "rangeexception",
        "rangeerrorstring",
        "rangefunc",
    ],
)


def cast(
    value,
    castfunction,
    caughtexceptions=(ValueError,),
    thrownexception=ISOFormatError,
    thrownmessage=None,
):
    try:
        result = castfunction(value)
    except caughtexceptions:
        raise thrownexception(thrownmessage)

    return result


def range_check(valuestr, limit):
    # Returns cast value if in range, raises defined exceptions on failure
    if valuestr is None:
        return None

    if "." in valuestr:
        castfunc = float
    else:
        castfunc = int

    value = cast(valuestr, castfunc, thrownmessage=limit.casterrorstring)

    if limit.min is not None and value < limit.min:
        raise limit.rangeexception(limit.rangeerrorstring)

    if limit.max is not None and value > limit.max:
        raise limit.rangeexception(limit.rangeerrorstring)

    return value


class BaseTimeBuilder(object):
    # Limit tuple format cast function, cast error string,
    # lower limit, upper limit, limit error string
    DATE_YYYY_LIMIT = Limit(
        "Invalid year string.",
        0000,
        9999,
        YearOutOfBoundsError,
        "Year must be between 1..9999.",
        range_check,
    )
    DATE_MM_LIMIT = Limit(
        "Invalid month string.",
        1,
        12,
        MonthOutOfBoundsError,
        "Month must be between 1..12.",
        range_check,
    )
    DATE_DD_LIMIT = Limit(
        "Invalid day string.",
        1,
        31,
        DayOutOfBoundsError,
        "Day must be between 1..31.",
        range_check,
    )
    DATE_WWW_LIMIT = Limit(
        "Invalid week string.",
        1,
        53,
        WeekOutOfBoundsError,
        "Week number must be between 1..53.",
        range_check,
    )
    DATE_D_LIMIT = Limit(
        "Invalid weekday string.",
        1,
        7,
        DayOutOfBoundsError,
        "Weekday number must be between 1..7.",
        range_check,
    )
    DATE_DDD_LIMIT = Limit(
        "Invalid ordinal day string.",
        1,
        366,
        DayOutOfBoundsError,
        "Ordinal day must be between 1..366.",
        range_check,
    )
    TIME_HH_LIMIT = Limit(
        "Invalid hour string.",
        0,
        24,
        HoursOutOfBoundsError,
        "Hour must be between 0..24 with " "24 representing midnight.",
        range_check,
    )
    TIME_MM_LIMIT = Limit(
        "Invalid minute string.",
        0,
        59,
        MinutesOutOfBoundsError,
        "Minute must be between 0..59.",
        range_check,
    )
    TIME_SS_LIMIT = Limit(
        "Invalid second string.",
        0,
        60,
        SecondsOutOfBoundsError,
        "Second must be between 0..60 with " "60 representing a leap second.",
        range_check,
    )
    TZ_HH_LIMIT = Limit(
        "Invalid timezone hour string.",
        0,
        23,
        HoursOutOfBoundsError,
        "Hour must be between 0..23.",
        range_check,
    )
    TZ_MM_LIMIT = Limit(
        "Invalid timezone minute string.",
        0,
        59,
        MinutesOutOfBoundsError,
        "Minute must be between 0..59.",
        range_check,
    )
    DURATION_PNY_LIMIT = Limit(
        "Invalid year duration string.",
        0,
        None,
        ISOFormatError,
        "Duration years component must be positive.",
        range_check,
    )
    DURATION_PNM_LIMIT = Limit(
        "Invalid month duration string.",
        0,
        None,
        ISOFormatError,
        "Duration months component must be positive.",
        range_check,
    )
    DURATION_PNW_LIMIT = Limit(
        "Invalid week duration string.",
        0,
        None,
        ISOFormatError,
        "Duration weeks component must be positive.",
        range_check,
    )
    DURATION_PND_LIMIT = Limit(
        "Invalid day duration string.",
        0,
        None,
        ISOFormatError,
        "Duration days component must be positive.",
        range_check,
    )
    DURATION_TNH_LIMIT = Limit(
        "Invalid hour duration string.",
        0,
        None,
        ISOFormatError,
        "Duration hours component must be positive.",
        range_check,
    )
    DURATION_TNM_LIMIT = Limit(
        "Invalid minute duration string.",
        0,
        None,
        ISOFormatError,
        "Duration minutes component must be positive.",
        range_check,
    )
    DURATION_TNS_LIMIT = Limit(
        "Invalid second duration string.",
        0,
        None,
        ISOFormatError,
        "Duration seconds component must be positive.",
        range_check,
    )
    INTERVAL_RNN_LIMIT = Limit(
        "Invalid duration repetition string.",
        0,
        None,
        ISOFormatError,
        "Duration repetition count must be positive.",
        range_check,
    )

    DATE_RANGE_DICT = {
        "YYYY": DATE_YYYY_LIMIT,
        "MM": DATE_MM_LIMIT,
        "DD": DATE_DD_LIMIT,
        "Www": DATE_WWW_LIMIT,
        "D": DATE_D_LIMIT,
        "DDD": DATE_DDD_LIMIT,
    }

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

    REPEATING_INTERVAL_RANGE_DICT = {"Rnn": INTERVAL_RNN_LIMIT}

    TIMEZONE_RANGE_DICT = {"hh": TZ_HH_LIMIT, "mm": TZ_MM_LIMIT}

    LEAP_SECONDS_SUPPORTED = False

    @classmethod
    def build_date(cls, YYYY=None, MM=None, DD=None, Www=None, D=None, DDD=None):
        raise NotImplementedError

    @classmethod
    def build_time(cls, hh=None, mm=None, ss=None, tz=None):
        raise NotImplementedError

    @classmethod
    def build_datetime(cls, date, time):
        raise NotImplementedError

    @classmethod
    def build_duration(
        cls, PnY=None, PnM=None, PnW=None, PnD=None, TnH=None, TnM=None, TnS=None
    ):
        raise NotImplementedError

    @classmethod
    def build_interval(cls, start=None, end=None, duration=None):
        # start, end, and duration are all tuples
        raise NotImplementedError

    @classmethod
    def build_repeating_interval(cls, R=None, Rnn=None, interval=None):
        # interval is a tuple
        raise NotImplementedError

    @classmethod
    def build_timezone(cls, negative=None, Z=None, hh=None, mm=None, name=""):
        raise NotImplementedError

    @classmethod
    def range_check_date(
        cls, YYYY=None, MM=None, DD=None, Www=None, D=None, DDD=None, rangedict=None
    ):
        if rangedict is None:
            rangedict = cls.DATE_RANGE_DICT

        if "YYYY" in rangedict:
            YYYY = rangedict["YYYY"].rangefunc(YYYY, rangedict["YYYY"])

        if "MM" in rangedict:
            MM = rangedict["MM"].rangefunc(MM, rangedict["MM"])

        if "DD" in rangedict:
            DD = rangedict["DD"].rangefunc(DD, rangedict["DD"])

        if "Www" in rangedict:
            Www = rangedict["Www"].rangefunc(Www, rangedict["Www"])

        if "D" in rangedict:
            D = rangedict["D"].rangefunc(D, rangedict["D"])

        if "DDD" in rangedict:
            DDD = rangedict["DDD"].rangefunc(DDD, rangedict["DDD"])

        if DD is not None:
            # Check calendar
            if DD > calendar.monthrange(YYYY, MM)[1]:
                raise DayOutOfBoundsError(
                    "{0} is out of range for {1}-{2}".format(DD, YYYY, MM)
                )

        if DDD is not None:
            if calendar.isleap(YYYY) is False and DDD == 366:
                raise DayOutOfBoundsError(
                    "{0} is only valid for leap year.".format(DDD)
                )

        return (YYYY, MM, DD, Www, D, DDD)

    @classmethod
    def range_check_time(cls, hh=None, mm=None, ss=None, tz=None, rangedict=None):
        # Used for midnight and leap second handling
        midnight = False  # Handle hh = '24' specially

        if rangedict is None:
            rangedict = cls.TIME_RANGE_DICT

        if "hh" in rangedict:
            try:
                hh = rangedict["hh"].rangefunc(hh, rangedict["hh"])
            except HoursOutOfBoundsError as e:
                if float(hh) > 24 and float(hh) < 25:
                    raise MidnightBoundsError("Hour 24 may only represent midnight.")

                raise e

        if "mm" in rangedict:
            mm = rangedict["mm"].rangefunc(mm, rangedict["mm"])

        if "ss" in rangedict:
            ss = rangedict["ss"].rangefunc(ss, rangedict["ss"])

        if hh is not None and hh == 24:
            midnight = True

        # Handle midnight range
        if midnight is True and (
            (mm is not None and mm != 0) or (ss is not None and ss != 0)
        ):
            raise MidnightBoundsError("Hour 24 may only represent midnight.")

        if cls.LEAP_SECONDS_SUPPORTED is True:
            if hh != 23 and mm != 59 and ss == 60:
                raise cls.TIME_SS_LIMIT.rangeexception(
                    cls.TIME_SS_LIMIT.rangeerrorstring
                )
        else:
            if hh == 23 and mm == 59 and ss == 60:
                # https://bitbucket.org/nielsenb/aniso8601/issues/10/sub-microsecond-precision-in-durations-is
                raise LeapSecondError("Leap seconds are not supported.")

            if ss == 60:
                raise cls.TIME_SS_LIMIT.rangeexception(
                    cls.TIME_SS_LIMIT.rangeerrorstring
                )

        return (hh, mm, ss, tz)

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
        if rangedict is None:
            rangedict = cls.DURATION_RANGE_DICT

        if "PnY" in rangedict:
            PnY = rangedict["PnY"].rangefunc(PnY, rangedict["PnY"])

        if "PnM" in rangedict:
            PnM = rangedict["PnM"].rangefunc(PnM, rangedict["PnM"])

        if "PnW" in rangedict:
            PnW = rangedict["PnW"].rangefunc(PnW, rangedict["PnW"])

        if "PnD" in rangedict:
            PnD = rangedict["PnD"].rangefunc(PnD, rangedict["PnD"])

        if "TnH" in rangedict:
            TnH = rangedict["TnH"].rangefunc(TnH, rangedict["TnH"])

        if "TnM" in rangedict:
            TnM = rangedict["TnM"].rangefunc(TnM, rangedict["TnM"])

        if "TnS" in rangedict:
            TnS = rangedict["TnS"].rangefunc(TnS, rangedict["TnS"])

        return (PnY, PnM, PnW, PnD, TnH, TnM, TnS)

    @classmethod
    def range_check_repeating_interval(
        cls, R=None, Rnn=None, interval=None, rangedict=None
    ):
        if rangedict is None:
            rangedict = cls.REPEATING_INTERVAL_RANGE_DICT

        if "Rnn" in rangedict:
            Rnn = rangedict["Rnn"].rangefunc(Rnn, rangedict["Rnn"])

        return (R, Rnn, interval)

    @classmethod
    def range_check_timezone(
        cls, negative=None, Z=None, hh=None, mm=None, name="", rangedict=None
    ):
        if rangedict is None:
            rangedict = cls.TIMEZONE_RANGE_DICT

        if "hh" in rangedict:
            hh = rangedict["hh"].rangefunc(hh, rangedict["hh"])

        if "mm" in rangedict:
            mm = rangedict["mm"].rangefunc(mm, rangedict["mm"])

        return (negative, Z, hh, mm, name)

    @classmethod
    def _build_object(cls, parsetuple):
        # Given a TupleBuilder tuple, build the correct object
        if type(parsetuple) is DateTuple:
            return cls.build_date(
                YYYY=parsetuple.YYYY,
                MM=parsetuple.MM,
                DD=parsetuple.DD,
                Www=parsetuple.Www,
                D=parsetuple.D,
                DDD=parsetuple.DDD,
            )

        if type(parsetuple) is TimeTuple:
            return cls.build_time(
                hh=parsetuple.hh, mm=parsetuple.mm, ss=parsetuple.ss, tz=parsetuple.tz
            )

        if type(parsetuple) is DatetimeTuple:
            return cls.build_datetime(parsetuple.date, parsetuple.time)

        if type(parsetuple) is DurationTuple:
            return cls.build_duration(
                PnY=parsetuple.PnY,
                PnM=parsetuple.PnM,
                PnW=parsetuple.PnW,
                PnD=parsetuple.PnD,
                TnH=parsetuple.TnH,
                TnM=parsetuple.TnM,
                TnS=parsetuple.TnS,
            )

        if type(parsetuple) is IntervalTuple:
            return cls.build_interval(
                start=parsetuple.start, end=parsetuple.end, duration=parsetuple.duration
            )

        if type(parsetuple) is RepeatingIntervalTuple:
            return cls.build_repeating_interval(
                R=parsetuple.R, Rnn=parsetuple.Rnn, interval=parsetuple.interval
            )

        return cls.build_timezone(
            negative=parsetuple.negative,
            Z=parsetuple.Z,
            hh=parsetuple.hh,
            mm=parsetuple.mm,
            name=parsetuple.name,
        )

    @classmethod
    def _is_interval_end_concise(cls, endtuple):
        if type(endtuple) is TimeTuple:
            return True

        if type(endtuple) is DatetimeTuple:
            enddatetuple = endtuple.date
        else:
            enddatetuple = endtuple

        if enddatetuple.YYYY is None:
            return True

        return False

    @classmethod
    def _combine_concise_interval_tuples(cls, starttuple, conciseendtuple):
        starttimetuple = None
        startdatetuple = None

        endtimetuple = None
        enddatetuple = None

        if type(starttuple) is DateTuple:
            startdatetuple = starttuple
        else:
            # Start is a datetime
            starttimetuple = starttuple.time
            startdatetuple = starttuple.date

        if type(conciseendtuple) is DateTuple:
            enddatetuple = conciseendtuple
        elif type(conciseendtuple) is DatetimeTuple:
            enddatetuple = conciseendtuple.date
            endtimetuple = conciseendtuple.time
        else:
            # Time
            endtimetuple = conciseendtuple

        if enddatetuple is not None:
            if enddatetuple.YYYY is None and enddatetuple.MM is None:
                newenddatetuple = DateTuple(
                    YYYY=startdatetuple.YYYY,
                    MM=startdatetuple.MM,
                    DD=enddatetuple.DD,
                    Www=enddatetuple.Www,
                    D=enddatetuple.D,
                    DDD=enddatetuple.DDD,
                )
            else:
                newenddatetuple = DateTuple(
                    YYYY=startdatetuple.YYYY,
                    MM=enddatetuple.MM,
                    DD=enddatetuple.DD,
                    Www=enddatetuple.Www,
                    D=enddatetuple.D,
                    DDD=enddatetuple.DDD,
                )

        if (starttimetuple is not None and starttimetuple.tz is not None) and (
            endtimetuple is not None and endtimetuple.tz != starttimetuple.tz
        ):
            # Copy the timezone across
            endtimetuple = TimeTuple(
                hh=endtimetuple.hh,
                mm=endtimetuple.mm,
                ss=endtimetuple.ss,
                tz=starttimetuple.tz,
            )

        if enddatetuple is not None and endtimetuple is None:
            return newenddatetuple

        if enddatetuple is not None and endtimetuple is not None:
            return TupleBuilder.build_datetime(newenddatetuple, endtimetuple)

        return TupleBuilder.build_datetime(startdatetuple, endtimetuple)


class TupleBuilder(BaseTimeBuilder):
    # Builder used to return the arguments as a tuple, cleans up some parse methods
    @classmethod
    def build_date(cls, YYYY=None, MM=None, DD=None, Www=None, D=None, DDD=None):

        return DateTuple(YYYY, MM, DD, Www, D, DDD)

    @classmethod
    def build_time(cls, hh=None, mm=None, ss=None, tz=None):
        return TimeTuple(hh, mm, ss, tz)

    @classmethod
    def build_datetime(cls, date, time):
        return DatetimeTuple(date, time)

    @classmethod
    def build_duration(
        cls, PnY=None, PnM=None, PnW=None, PnD=None, TnH=None, TnM=None, TnS=None
    ):

        return DurationTuple(PnY, PnM, PnW, PnD, TnH, TnM, TnS)

    @classmethod
    def build_interval(cls, start=None, end=None, duration=None):
        return IntervalTuple(start, end, duration)

    @classmethod
    def build_repeating_interval(cls, R=None, Rnn=None, interval=None):
        return RepeatingIntervalTuple(R, Rnn, interval)

    @classmethod
    def build_timezone(cls, negative=None, Z=None, hh=None, mm=None, name=""):
        return TimezoneTuple(negative, Z, hh, mm, name)
