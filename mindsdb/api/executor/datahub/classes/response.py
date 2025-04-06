from dataclasses import dataclass, field
from typing import Optional, List, Dict

import pandas as pd


@dataclass
class DataHubResponse:
    data_frame: pd.DataFrame = field(default_factory=pd.DataFrame)
    columns: List[Dict] = field(default_factory=list)
    affected_rows: Optional[int] = None
