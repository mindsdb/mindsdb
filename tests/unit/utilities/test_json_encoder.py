import orjson
import pandas as pd
from datetime import datetime, date, timedelta
from decimal import Decimal

from mindsdb.utilities.json_encoder import CustomJSONEncoder


DEFAULT = CustomJSONEncoder().default


def dumps(payload):
    return orjson.dumps(
        payload,
        default=DEFAULT,
        option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NON_STR_KEYS | orjson.OPT_PASSTHROUGH_DATETIME,
    ).decode("utf-8")


def test_date_and_datetime_and_timedelta():
    s = dumps(
        {
            "d": date(2024, 7, 9),
            "dt": datetime(2024, 7, 9, 1, 2, 3, 0),
            "td": timedelta(hours=1, minutes=2, seconds=3),
        }
    )
    assert '"d":"2024-07-09"' in s
    assert '"dt":"2024-07-09 01:02:03.000000"' in s
    assert '"td":"1:02:03"' in s


def test_pandas_na_serializes_to_null():
    s = dumps({"x": pd.NA})
    assert '"x":null' in s


def test_decimal_serialization_to_number():
    # Our default maps Decimal to float
    s = dumps({"price": Decimal("12.34")})
    assert '"price":12.34' in s
