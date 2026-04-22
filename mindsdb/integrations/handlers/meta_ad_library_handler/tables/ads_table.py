from __future__ import annotations

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource


class AdsTable(APIResource):
    COLUMNS = [
        "id",
        "ad_creation_time",
        "ad_creative_bodies",
        "ad_creative_link_captions",
        "ad_creative_link_descriptions",
        "ad_creative_link_titles",
        "ad_delivery_start_time",
        "ad_delivery_stop_time",
        "ad_snapshot_url",
        "bylines",
        "currency",
        "delivery_by_region",
        "demographic_distribution",
        "estimated_audience_size",
        "impressions",
        "languages",
        "page_id",
        "page_name",
        "publisher_platforms",
        "spend",
    ]

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        rows = self.handler.fetch_ads(conditions=conditions, limit=limit, sort=sort)
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
