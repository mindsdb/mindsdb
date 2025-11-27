from __future__ import annotations

from typing import List, Optional

import math

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn


class PocketbaseTable(APIResource):
    """Implements CRUD operations for a PocketBase collection."""

    DEFAULT_PAGE_SIZE = 200

    def __init__(self, handler, collection: dict):
        super().__init__(handler, table_name=collection["name"])
        self.collection = collection
        self.columns = self._build_columns()

    def _build_columns(self) -> List[str]:
        base_columns = ["id", "created", "updated", "collectionId", "collectionName"]
        schema = self.collection.get("schema", [])
        for field in schema:
            field_name = field.get("name")
            if field_name and field_name not in base_columns:
                base_columns.append(field_name)
        return base_columns

    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[SortColumn]] = None,
        targets: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch records from the collection."""

        self.handler.connect()
        records: List[dict] = []
        page = 1
        remaining = limit

        while True:
            per_page = self.DEFAULT_PAGE_SIZE
            if remaining is not None:
                per_page = min(per_page, max(remaining - len(records), 0))
                if per_page == 0:
                    break

            params = {"page": page, "perPage": per_page or self.DEFAULT_PAGE_SIZE}
            filter_expression = self._build_filter(conditions or [])
            if filter_expression:
                params["filter"] = filter_expression

            sort_expression = self._build_sort(sort or [])
            if sort_expression:
                params["sort"] = sort_expression

            if targets:
                params["fields"] = ",".join(targets)

            response = self.handler._request(
                "GET",
                f"/api/collections/{self.table_name}/records",
                params=params,
            )

            items = response.get("items", [])
            if not items:
                break

            records.extend(items)
            if remaining is not None and len(records) >= remaining:
                break

            total_items = response.get("totalItems")
            response_page = response.get("page", page)
            response_per_page = response.get("perPage") or params["perPage"]
            total_pages = None
            if total_items is not None and response_per_page:
                total_pages = math.ceil(total_items / response_per_page)

            if total_pages is not None and response_page >= total_pages:
                break

            if len(items) < response_per_page:
                break

            page += 1

        df = pd.DataFrame(records)
        if df.empty:
            return pd.DataFrame(columns=targets or self.columns)

        return df

    def add(self, rows: List[dict], **kwargs) -> None:
        """Insert new records."""

        for row in rows:
            self.handler._request("POST", f"/api/collections/{self.table_name}/records", json=row)

    def modify(self, conditions: List[FilterCondition], values: dict):
        """Update records that match the given id conditions."""

        record_ids = self._resolve_record_ids(conditions)
        if not record_ids:
            raise ValueError("Updates require filtering by the record id column.")

        for record_id in record_ids:
            self.handler._request(
                "PATCH",
                f"/api/collections/{self.table_name}/records/{record_id}",
                json=values,
            )

    def remove(self, conditions: List[FilterCondition]):
        """Delete records that match the given id conditions."""

        record_ids = self._resolve_record_ids(conditions)
        if not record_ids:
            raise ValueError("Deletes require filtering by the record id column.")

        for record_id in record_ids:
            self.handler._request(
                "DELETE",
                f"/api/collections/{self.table_name}/records/{record_id}",
            )

    def get_columns(self) -> List[str]:
        return self.columns

    def _resolve_record_ids(self, conditions: List[FilterCondition]) -> List[str]:
        record_ids: List[str] = []
        for condition in conditions or []:
            if condition.column.lower() != "id":
                continue
            if condition.op == FilterOperator.EQUAL:
                record_ids.append(str(condition.value))
            elif condition.op == FilterOperator.IN and isinstance(condition.value, list):
                record_ids.extend([str(value) for value in condition.value])
        return record_ids

    def _build_filter(self, conditions: List[FilterCondition]) -> Optional[str]:
        expressions: List[str] = []
        for condition in conditions:
            operator = self._map_operator(condition.op)
            if operator is None:
                continue

            value = self._format_value(condition.value)
            expressions.append(f"{condition.column}{operator}{value}")
            condition.applied = True

        if expressions:
            return " && ".join(expressions)

        return None

    def _map_operator(self, operator: FilterOperator) -> Optional[str]:
        mapping = {
            FilterOperator.EQUAL: "=",
            FilterOperator.NOT_EQUAL: "!=",
            FilterOperator.GREATER_THAN: ">",
            FilterOperator.GREATER_THAN_OR_EQUAL: ">=",
            FilterOperator.LESS_THAN: "<",
            FilterOperator.LESS_THAN_OR_EQUAL: "<=",
        }
        return mapping.get(operator)

    def _format_value(self, value) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if value is None:
            return "null"
        if isinstance(value, (int, float)):
            return str(value)
        escaped = str(value).replace("'", "\\'")
        return f"'{escaped}'"

    def _build_sort(self, sort_columns: List[SortColumn]) -> Optional[str]:
        expressions: List[str] = []
        for column in sort_columns:
            direction = "" if column.ascending else "-"
            expressions.append(f"{direction}{column.column}")
            column.applied = True

        if expressions:
            return ",".join(expressions)

        return None
