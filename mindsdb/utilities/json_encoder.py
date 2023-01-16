import base64
from datetime import datetime, date, timedelta
from decimal import Decimal
import numpy as np
from flask.json import JSONEncoder


class CustomJSONEncoder(JSONEncoder):
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

        return str(obj)


def json_serialiser(byte_obj):
    """
    Used to export/import predictors inside the model controller.
    Reference: https://stackoverflow.com/q/53942948.
    """
    if isinstance(byte_obj, (bytes, bytearray)):
        # File Bytes to Base64 Bytes then to String
        return base64.b64encode(byte_obj).decode('utf-8')
    raise ValueError('No encoding handler for data type ' + type(byte_obj))
