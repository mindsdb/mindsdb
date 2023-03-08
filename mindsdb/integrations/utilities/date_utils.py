import datetime as dt
import pytz


def parse_utc_date(date_str: str) -> dt.datetime:
    if isinstance(date_str, dt.datetime):
        return date_str
    date_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']
    date = None
    for date_format in date_formats:
        try:
            date = dt.datetime.strptime(date_str, date_format)
        except ValueError:
            pass
    if date is None:
        raise ValueError(f"Can't parse date: {date_str}")
    date = date.astimezone(pytz.utc)
    return date


def utc_date_str_to_timestamp_ms(date_str: str) -> int:
    date = parse_utc_date(date_str)
    # `timestamp` method doesn't work as expected unless we replace the timezone info this way.
    date = date.replace(tzinfo=pytz.UTC)
    return int(date.timestamp()) * 1000
