from __future__ import annotations

from typing import Any

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource


class CampaignAnalyticsTable(APIResource):
    COLUMNS = [
        "campaign_urn",
        "campaign_id",
        "date_start",
        "date_end",
        "time_granularity",
        "impressions",
        "clicks",
        "landing_page_clicks",
        "likes",
        "shares",
        "cost_in_local_currency",
        "external_website_conversions",
    ]

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        rows = self.handler.fetch_campaign_analytics(conditions=conditions, limit=limit, sort=sort)
        dataframe = pd.DataFrame(rows)
        if dataframe.empty:
            dataframe = pd.DataFrame(columns=self.COLUMNS)
        else:
            for column in self.COLUMNS:
                if column not in dataframe.columns:
                    dataframe[column] = None
            dataframe = dataframe[self.COLUMNS]

        required_columns = self._get_required_columns(conditions=conditions, sort=sort)
        if targets:
            selected = [column for column in targets if column in dataframe.columns]
            selected += [
                column
                for column in required_columns
                if column in dataframe.columns and column not in selected
            ]
            if selected:
                dataframe = dataframe[selected]

        return dataframe

    @staticmethod
    def _get_required_columns(conditions: list[Any] | None, sort: list[Any] | None) -> list[str]:
        required_columns: list[str] = []

        for condition in conditions or []:
            column = getattr(condition, "column", None)
            if column and column not in required_columns:
                required_columns.append(column)

        for sort_column in sort or []:
            column = getattr(sort_column, "column", None)
            if column and column not in required_columns:
                required_columns.append(column)

        return required_columns
