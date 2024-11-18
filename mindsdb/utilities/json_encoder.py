from datetime import datetime, date, timedelta
from decimal import Decimal
import numpy as np
import pandas as pd
import json

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
        if isinstance(obj, np.int8) or isinstance(obj, np.int16) or isinstance(obj, np.int32) or isinstance(obj, np.int64):
            return int(obj)
        if isinstance(obj, np.float16) or isinstance(obj, np.float32) or isinstance(obj, np.float64) or isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if pd.isnull(obj):
            return None

        return str(obj)


class CustomJSONProvider(CustomJSONEncoder, DefaultJSONProvider):
    ...
