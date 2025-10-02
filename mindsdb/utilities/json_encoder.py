from datetime import datetime, date, timedelta
from decimal import Decimal
import pandas as pd
import numpy as np
import json
import orjson
from flask.json.provider import DefaultJSONProvider


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, timedelta):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S.%f")
        if isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        if isinstance(obj, np.bool_):
            return bool(obj)
        if pd.isnull(obj):
            return None

        return str(obj)


class ORJSONProvider(DefaultJSONProvider):
    """
    Use orjson to serialize data instead of flask json provider.
    """

    def dumps(self, obj, **kwargs):
        return orjson.dumps(
            obj,
            option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NON_STR_KEYS,
            default=CustomJSONEncoder().default,
        ).decode("utf-8")

    def loads(self, s, **kwargs):
        return orjson.loads(s)
