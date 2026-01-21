from typing import List, Optional

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class NetSuiteRecordTable(APIResource):
    """
    Table abstraction for a NetSuite record type.
    """

    def __init__(self, handler, record_type: str):
        """
        Initializes the record table.

        Args:
            handler: NetSuite handler instance.
            record_type (str): NetSuite record type.
        """
        self.record_type = record_type
        super().__init__(handler, table_name=record_type)

    @property
    def _base_path(self) -> str:
        return f"/services/rest/record/v1/{self.record_type}"

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: Optional[int] = None,
        sort: Optional[list] = None,
        targets: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Fetches records from NetSuite.

        Args:
            conditions (List[FilterCondition]): Optional filter conditions.
            limit (Optional[int]): Optional maximum number of records.
            sort (Optional[list]): Optional sort columns.
            targets (Optional[List[str]]): Optional target columns.
            **kwargs: Additional arguments.

        Returns:
            pd.DataFrame: Records from NetSuite.
        """
        limit = int(limit) if limit is not None else None
        items = []
        next_url = self._base_path
        remaining = limit

        # If filtering on a specific id/internalId, fetch the full record directly.
        record_id = None
        for cond in conditions or []:
            if cond.op == FilterOperator.EQUAL and cond.column.lower() in ("id", "internalid"):
                record_id = cond.value
                break
        if record_id is not None:
            record_url = f"{self._base_path}/{record_id}"
            try:
                payload = self.handler._request("GET", record_url)
            except RuntimeError:
                if targets:
                    return pd.DataFrame(columns=targets)
                return pd.DataFrame(columns=["internalId"])
            if isinstance(payload, dict):
                return pd.DataFrame([payload])
            if targets:
                return pd.DataFrame(columns=targets)
            return pd.DataFrame(columns=["internalId"])

        def _format_q_value(value):
            if value is None:
                return "NULL"
            if isinstance(value, bool):
                return "true" if value else "false"
            if isinstance(value, (int, float)):
                return str(value)
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"

        def _normalize_column(column: str) -> str:
            return "internalId" if column.lower() == "id" else column

        q_filters = []
        for cond in conditions or []:
            if cond.op == FilterOperator.EQUAL:
                column = _normalize_column(cond.column)
                if cond.value is None:
                    q_filters.append(f"{column} IS NULL")
                else:
                    q_filters.append(f"{column} = {_format_q_value(cond.value)}")
                cond.applied = True

        while next_url:
            if remaining is not None and remaining <= 0:
                break
            page_limit = remaining if remaining is not None else None
            params = {}
            if page_limit is not None:
                params["limit"] = page_limit if page_limit > 0 else 1
            if q_filters:
                params["q"] = " AND ".join(q_filters)
            order_by = None
            if sort:
                order_by = ",".join([f"{col.column}:{'asc' if col.ascending else 'desc'}" for col in sort])
                params["orderBy"] = order_by

            try:
                payload = self.handler._request("GET", next_url, params=params)
            except RuntimeError as exc:
                if order_by and "orderBy" in params:
                    params.pop("orderBy", None)
                    payload = self.handler._request("GET", next_url, params=params)
                else:
                    raise exc

            if isinstance(payload, dict):
                page_items = payload.get("items") or payload.get("data") or payload.get("results") or []
                links = payload.get("links") or []
                next_url = None
                for link in links:
                    if isinstance(link, dict) and link.get("rel") == "next":
                        next_url = link.get("href")
                        break
            elif isinstance(payload, list):
                page_items = payload
                next_url = None
            else:
                page_items = []
                next_url = None

            items.extend(page_items)

            if remaining is not None:
                remaining -= len(page_items)
                if remaining <= 0:
                    break

            if not page_items:
                break

        df = pd.DataFrame(items)

        # DuckDB fails on SELECT * from a DataFrame with zero columns; ensure at least one column exists on empty result sets.
        if df.empty:
            if targets:
                df = pd.DataFrame(columns=targets)
            else:
                df = pd.DataFrame(columns=["internalId"])

        if targets:
            df = df.reindex(columns=targets) if not df.empty else pd.DataFrame(columns=targets)
        return df

    def add(self, row: List[dict], **kwargs) -> None:
        """
        Creates records in NetSuite.

        Args:
            row (List[dict]): Records to add.
        """
        for payload in row:
            self.handler._request("POST", self._base_path, json=payload)

    def modify(self, conditions: List[FilterCondition], values: dict):
        """
        Updates records in NetSuite identified by id/internalId.

        Args:
            conditions (List[FilterCondition]): Conditions to select records.
            values (dict): Updated values.
        """
        record_ids = [
            cond.value
            for cond in conditions or []
            if cond.op == FilterOperator.EQUAL and cond.column.lower() in ("id", "internalid")
        ]
        if not record_ids:
            raise ValueError("Update requires an equality condition on 'id' or 'internalId'.")

        for record_id in record_ids:
            path = f"{self._base_path}/{record_id}"
            self.handler._request("PATCH", path, json=values)

    def remove(self, conditions: List[FilterCondition]):
        """
        Deletes records in NetSuite identified by id/internalId.

        Args:
            conditions (List[FilterCondition]): Conditions to select records.
        """
        record_ids = [
            cond.value
            for cond in conditions or []
            if cond.op == FilterOperator.EQUAL and cond.column.lower() in ("id", "internalid")
        ]
        if not record_ids:
            raise ValueError("Delete requires an equality condition on 'id' or 'internalId'.")

        for record_id in record_ids:
            path = f"{self._base_path}/{record_id}"
            self.handler._request("DELETE", path)

    def get_columns(self) -> list:
        """
        Infers columns from a sample response.

        Returns:
            list: List of column names.
        """
        sample = self.list(limit=1)
        if sample.empty:
            return []
        return list(sample.columns)
