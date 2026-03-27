from __future__ import annotations

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource


class CampaignsTable(APIResource):
    COLUMNS = [
        "id",
        "name",
        "status",
        "type",
        "test",
        "account_urn",
        "account_id",
        "campaign_group_urn",
        "campaign_group_id",
        "associated_entity_urn",
        "cost_type",
        "creative_selection",
        "objective_type",
        "optimization_target_type",
        "format",
        "locale_country",
        "locale_language",
        "audience_expansion_enabled",
        "offsite_delivery_enabled",
        "serving_statuses",
        "daily_budget_amount",
        "daily_budget_currency_code",
        "total_budget_amount",
        "total_budget_currency_code",
        "unit_cost_amount",
        "unit_cost_currency_code",
        "run_schedule_start",
        "run_schedule_end",
        "created_at",
        "last_modified_at",
    ]

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        rows = self.handler.fetch_campaigns(conditions=conditions, limit=limit, sort=sort)
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
