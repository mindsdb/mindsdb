from typing import List

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition,
    FilterOperator,
    SortColumn
)


class ChannelsTable(APIResource):
    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs
    ):
        client = self.handler.connect()
        channels = []

        channels = client.get_channels()
        channels_df = pd.json_normalize(channels)

        return channels_df

    def get_columns(self) -> List[str]:
        return [
            "id",
            "createdDateTime",
            "displayName",
            "description",
            "isFavoriteByDefault",
            "email",
            "tenantId",
            "webUrl",
            "membershipType",
            "teamId",
        ]
