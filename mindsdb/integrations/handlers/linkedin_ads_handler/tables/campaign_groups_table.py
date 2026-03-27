from __future__ import annotations

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource


class CampaignGroupsTable(APIResource):
    COLUMNS = [
        "id",
        "name",
        "status",
        "test",
        "account_urn",
        "account_id",
        "serving_statuses",
        "allowed_campaign_types",
        "total_budget_amount",
        "total_budget_currency_code",
        "daily_budget_amount",
        "daily_budget_currency_code",
        "run_schedule_start",
        "run_schedule_end",
        "created_at",
        "last_modified_at",
    ]

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        rows = self.handler.fetch_campaign_groups(conditions=conditions, limit=limit, sort=sort)
        dataframe = pd.DataFrame(rows)
        if dataframe.empty:
            dataframe = pd.DataFrame(columns=self.COLUMNS)
        else:
            for column in self.COLUMNS:
                if column not in dataframe.columns:
                    dataframe[column] = None
            dataframe = dataframe[self.COLUMNS]

        if targets:
            selected = [column for column in targets if column in dataframe.columns]
            if selected:
                dataframe = dataframe[selected]

        return dataframe
