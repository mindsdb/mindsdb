import calendar
import pytz
import re


__author__ = 'jeffrey.starr@ztoztechnologies.com'


class DateElement(object):
    """
    Abstract class for a date element, a portion of a valid date/time string

    Inheriting classes should implement a string 'directive' field that provides the relevant
    directive for the datetime.strftime/strptime method.
    """
    directive = None

    def __eq__(self, other):
        if other is None:
            return False
        return self.directive == other.directive

    def __hash__(self):
        return self.directive.__hash__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return self.directive

    def __str__(self):
        return self.directive

    @staticmethod
    def is_numerical():
        """
        Return true if the written representation of the element are digits
        """
        raise NotImplementedError('is_numerical')


class AMPM(DateElement):
    """AM | PM"""
    directive = '%p'

    @staticmethod
    def is_match(token):
        return token in ('AM', 'PM', 'am', 'pm')

    @staticmethod
    def is_numerical():
        return False


class DayOfMonth(DateElement):
    """1 .. 31"""

    directive = '%d'

    @staticmethod
    def is_match(token):
        try:
            day = int(token)
            return 1 <= day <= 31
        except ValueError:
            return False

    @staticmethod
    def is_numerical():
        return True


class Filler(DateElement):
    """
    A special date class, filler matches everything. Filler is usually used for matches of whitespace
    and punctuation.
    """
    def __init__(self, filler):
        self.directive = filler.replace('%', '%%')  # escape %

    @staticmethod
    def is_match(token):
        return True

    @staticmethod
    def is_numerical():
        return False


class Hour12(DateElement):
    """1 .. 12 (zero padding accepted)"""
    directive = '%I'

    @staticmethod
    def is_match(token):
        try:
            hour = int(token)
            return 1 <= hour <= 12
        except ValueError:
            return False

    @staticmethod
    def is_numerical():
        return True


class Hour24(DateElement):
    """00 .. 23"""
    directive = '%H'

    @staticmethod
    def is_match(token):
        try:
            hour = int(token)
            return 0 <= hour <= 23
        except ValueError:
            return False

    @staticmethod
    def is_numerical():
        return True


class Minute(DateElement):
    """00 .. 59"""
    directive = '%M'

    @staticmethod
    def is_match(token):
        try:
            minute = int(token)
            return 0 <= minute <= 59
        except ValueError:
            return False

    @staticmethod
    def is_numerical():
        return True


class MonthNum(DateElement):
    """1 .. 12"""

    directive = '%m'

    @staticmethod
    def is_match(token):
        try:
            month = int(token)
            return 1 <= month <= 12
        except ValueError:
            return False

    @staticmethod
    def is_numerical():
        return True


class MonthTextLong(DateElement):
    """January, February, ..., December

    Uses calendar.month_name to provide localization
    """
    directive = '%B'

    @staticmethod
    def is_match(token):
        return token in calendar.month_name

    @staticmethod
    def is_numerical():
        return False


class MonthTextShort(DateElement):
    """Jan, Feb, ... Dec

    Uses calendar.month_abbr to provide localization
    """
    directive = '%b'

    @staticmethod
    def is_match(token):
        return token in calendar.month_abbr

    @staticmethod
    def is_numerical():
        return False


class Second(DateElement):
    """00 .. 60

    Normally, seconds range from 0 to 59. In the case of a leap second, the second value may be 60.
    """
    directive = '%S'

    @staticmethod
    def is_match(token):
        try:
            second = int(token)
            return 0 <= second <= 60
        except ValueError:
            return False

    @staticmethod
    def is_numerical():
        return True


class Timezone(DateElement):
    """IANA common timezones (e.g. UTC, EST, US/Eastern, ...)"""
    directive = '%Z'

    @staticmethod
    def is_match(token):
        return token in pytz.all_timezones_set

    @staticmethod
    def is_numerical():
        return False


class UTCOffset(DateElement):
    """UTC offset +0400 -1130"""
    directive = '%z'

    @staticmethod
    def is_match(token):
        # technically offset_re should be:
        # ^[-\+]\d\d:?(\d\d)?$
        # but python apparently only uses the +/-hhmm format
        # A rule will catch the preceding + and - and combine the two entries since punctuation and numbers
        # are separated by the tokenizer.
        offset_re = r'^\d\d\d\d$'
        return re.match(offset_re, token)

    @staticmethod
    def is_numerical():
        return False


class WeekdayLong(DateElement):
    """Sunday, Monday, ..., Saturday

    Uses calendar.day_name to provide localization
    """
    directive = '%A'

    @staticmethod
    def is_match(token):
        return token in calendar.day_name


class WeekdayShort(DateElement):
    """Sun, Mon, ... Sat

    Uses calendar.day_abbr to provide localization
    """
    directive = '%a'

    @staticmethod
    def is_match(token):
        return token in calendar.day_abbr

    @staticmethod
    def is_numerical():
        return False


class Year2(DateElement):
    """00 .. 99"""

    directive = '%y'

    @staticmethod
    def is_match(token):
        if len(token) != 2:
            return False
        try:
            year = int(token)
            return 0 <= year <= 99
        except ValueError:
            return False

    @staticmethod
    def is_numerical():
        return True


class Year4(DateElement):
    """0000 .. 9999"""

    directive = '%Y'

    @staticmethod
    def is_match(token):
        if len(token) != 4:
            return False
        try:
            year = int(token)
            return True
        except ValueError:
            return False

    @staticmethod
    def is_numerical():
        return True
