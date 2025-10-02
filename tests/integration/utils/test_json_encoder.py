import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import pytest
from flask import Flask

from mindsdb.utilities.json_encoder import ORJSONProvider


@pytest.fixture()
def prov():
    app = Flask(__name__)
    app.json = ORJSONProvider(app)
    with app.app_context():
        yield app.json


def test_dates_and_timedelta_serialization(prov):
    payload = {
        "d": date(2024, 1, 2),
        "dt": datetime(2024, 1, 2, 3, 4, 5),
        "td": timedelta(hours=1, minutes=2, seconds=3),
    }
    s = prov.dumps(payload)
    assert '"2024-01-02"' in s
    assert "01:02:03" in s


def test_numpy_scalars_and_arrays(prov):
    payload = {
        "b": np.bool_(True),
        "i": np.int64(42),
        "f": np.float64(3.14),
        "arr": np.array([1, 2, 3], dtype=np.int32),
    }
    s = prov.dumps(payload)
    # orjson with OPT_SERIALIZE_NUMPY should serialize these
    assert '"arr":[1,2,3]' in s


def test_pandas_na_to_none(prov):
    """
    Test if it calls our CustomJSONEncoder.default
    """
    payload = {"x": pd.NA}
    s = prov.dumps(payload)
    assert '"x":null' in s


def test_date_serialization_format(prov):
    payload = {"d": date(2024, 7, 9)}
    s = prov.dumps(payload)
    assert '"d":"2024-07-09"' in s


def test_datetime_serialization_format(prov):
    dt = datetime(2024, 7, 9, 1, 2, 3, 0)
    payload = {"dt": dt}
    s = prov.dumps(payload)
    # Expect "%Y-%m-%d %H:%M:%S.%f" per CustomJSONEncoder
    assert '"dt":"2024-07-09 01:02:03.000000"' in s
