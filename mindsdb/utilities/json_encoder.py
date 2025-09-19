from datetime import datetime, date, timedelta
from decimal import Decimal
import numpy as np
import pandas as pd
import json

from flask.json.provider import DefaultJSONProvider


class CustomJSONEncoder(json.JSONEncoder):
def default(self, obj):
    # Check for pandas null first as it's common
    if pd.isnull(obj):
        return None
    
    # Group numpy types together for efficiency
    obj_type = type(obj)
    
    # Datetime types
    if obj_type is datetime:
        return obj.strftime("%Y-%m-%d %H:%M:%S.%f")
    if obj_type is date:
        return obj.strftime("%Y-%m-%d")
    if obj_type is timedelta:
        return str(obj)
    
    # NumPy types - use direct type comparison for better performance
    if obj_type is np.bool_:
        return bool(obj)
    if obj_type in (np.int8, np.int16, np.int32, np.int64):
        return int(obj)
    if obj_type in (np.float16, np.float32, np.float64) or obj_type is Decimal:
        return float(obj)
    if obj_type is np.ndarray:
        return obj.tolist()

    return str(obj)


class CustomJSONProvider(CustomJSONEncoder, DefaultJSONProvider):
    ...
