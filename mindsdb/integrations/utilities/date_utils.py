import datetime as dt
import pytz
import re


def parse_local_date(date_str: str) -> dt.datetime:
    """Parses common date string formats to local datetime objects."""
    if isinstance(date_str, dt.datetime):
        return date_str
    date_formats = ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']

    date = None
    for date_format in date_formats:
        try:
            date = dt.datetime.strptime(date_str, date_format)
        except ValueError:
            pass
    if date is None:
        raise ValueError(f"Can't parse date: {date_str}")
    return date


def parse_utc_date_with_limit(date_str: str, max_window_in_days: int = None) -> dt.datetime:
    """Parses common date string formats to UTC datetime objects."""
    date = parse_local_date(date_str)

    # Convert date to UTC
    date_utc = date.astimezone(pytz.utc)

    # If max_window_in_days is provided, apply the logic
    if max_window_in_days is not None:
        # Get the current UTC time
        now_utc = dt.datetime.utcnow().replace(tzinfo=pytz.utc)
        # Check if the parsed date is earlier than the maximum window allowed
        max_window_date = now_utc - dt.timedelta(days=max_window_in_days)
        if date_utc < max_window_date:
            return max_window_date
    return date_utc


def parse_utc_date(date_str: str) -> dt.datetime:
    """Parses common date string formats to UTC datetime objects."""
    date = parse_local_date(date_str)
    return date.astimezone(pytz.utc)


def utc_date_str_to_timestamp_ms(date_str: str) -> int:
    """Parses common date string formats into ms since the Unix epoch in UTC."""
    date = parse_local_date(date_str)
    # `timestamp` method doesn't work as expected unless we replace the timezone info this way.
    date = date.replace(tzinfo=pytz.UTC)
    return int(date.timestamp() * 1000)


def interval_str_to_duration_ms(interval_str: str) -> int:
    """Parses interval strings into how long they represent in ms.
    Supported intervals:
      - seconds (e.g. 1s)
      - minutes (e.g. 5m)
      - hours (e.g. 1h)
      - days (e.g. 5d)
      - weeks (e.g. 1w)
    """
    duration_match = re.search(r'^\d+', interval_str)
    time_unit_match = re.search('[smhdw]', interval_str)
    if not duration_match or not time_unit_match:
        raise ValueError('Invalid interval {}'.format(interval_str))
    duration = int(duration_match.group())
    time_unit = time_unit_match.group()
    if time_unit == 's':
        return duration * 1000
    if time_unit == 'm':
        return duration * 1000 * 60
    if time_unit == 'h':
        return duration * 1000 * 60 * 60
    if time_unit == 'd':
        return duration * 1000 * 60 * 60 * 24
    if time_unit == 'w':
        return duration * 1000 * 60 * 60 * 24 * 7
