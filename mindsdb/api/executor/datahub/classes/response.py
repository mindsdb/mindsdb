from dataclasses import dataclass, field
from typing import List, Dict

import pandas as pd

from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE


@dataclass
class DataHubResponse:
    data_frame: pd.DataFrame = field(default_factory=pd.DataFrame)
    columns: List[Dict] = field(default_factory=list)
    affected_rows: int | None = None
    mysql_types: list[MYSQL_DATA_TYPE] | None = None
