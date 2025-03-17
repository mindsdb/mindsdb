from typing import List

import pandas as pd

from mindsdb.integrations.handlers.ms_teams_handler.ms_graph_api_teams_client import MSGraphAPITeamsClient
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
        client: MSGraphAPITeamsClient = self.handler.connect()
        channels = []

        team_id = None
        channel_ids = None
        for condition in conditions:
            if condition.column == "teamId":
                if condition.op == FilterOperator.EQUAL:
                    team_id = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'teamId'."
                    )

                condition.applied = True

            if condition.column == "id":
                if condition.op == FilterOperator.EQUAL:
                    channel_ids = [condition.value]

                elif condition.op == FilterOperator.IN:
                    channel_ids = condition.value

                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.op}' for column 'id'."
                    )

                condition.applied = True

        if team_id:
            if channel_ids:
                channels = client.get_channels_in_group_by_ids(team_id, channel_ids)

            else:
                channels = client.get_all_channels_in_group(team_id)

        elif channel_ids:
            channels = client.get_channels_across_all_groups_by_ids(channel_ids)

        else:
            channels = client.get_all_channels_across_all_groups()

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
