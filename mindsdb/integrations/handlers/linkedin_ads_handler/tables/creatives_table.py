from __future__ import annotations

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource


class CreativesTable(APIResource):
    COLUMNS = [
        "id",
        "creative_id",
        "campaign_urn",
        "campaign_id",
        "account_urn",
        "account_id",
        "intended_status",
        "is_test",
        "is_serving",
        "serving_hold_reasons",
        "content_reference",
        "content_reference_id",
        "created_at",
        "last_modified_at",
        "created_by",
        "last_modified_by",
    ]

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        rows = self.handler.fetch_creatives(conditions=conditions, limit=limit, sort=sort)
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
