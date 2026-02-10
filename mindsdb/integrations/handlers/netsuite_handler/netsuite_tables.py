from typing import List, Optional

import pandas as pd

from mindsdb.integrations.libs.api_handler import MetaAPIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, filter_dataframe
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class NetSuiteRecordTable(MetaAPIResource):
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
        self.record_type = str(record_type).lower()
        self._resource_metadata = None
        super().__init__(handler, table_name=record_type)

    def _get_resource_metadata(self) -> dict:
        """
        Retrieves record metadata for this NetSuite record type.

        Returns:
            dict: Metadata dictionary when available, otherwise an empty dict.
        """
        if self._resource_metadata is not None:
            return self._resource_metadata

        metadata = None
        if hasattr(self.handler, "_get_record_metadata"):
            metadata = self.handler._get_record_metadata(self.record_type)

        self._resource_metadata = metadata or {}
        return self._resource_metadata

    @staticmethod
    def _normalize_metadata_type(value) -> str:
        if isinstance(value, dict):
            if "type" in value:
                return NetSuiteRecordTable._normalize_metadata_type(value.get("type"))
            if "$ref" in value and isinstance(value["$ref"], str):
                return value["$ref"].split("/")[-1]
            if "format" in value:
                return str(value["format"])
            return "str"
        if isinstance(value, list):
            return ",".join([str(item) for item in value])
        if value is None:
            return "str"
        return str(value)

    def _extract_field_metadata(self) -> List[dict]:
        """
        Extracts column metadata from the NetSuite metadata catalog response.

        Returns:
            List[dict]: Column metadata entries with table_name, column_name, data_type,
            column_description, is_nullable, and column_default.
        """
        metadata = self._get_resource_metadata()
        if not isinstance(metadata, dict) or not metadata:
            return []

        fields_metadata = []

        fields = metadata.get("fields")
        if isinstance(fields, list):
            for field in fields:
                if not isinstance(field, dict):
                    continue
                name = (
                    field.get("id")
                    or field.get("name")
                    or field.get("fieldId")
                    or field.get("key")
                    or field.get("scriptId")
                )
                if not name:
                    continue
                data_type = (
                    field.get("type") or field.get("dataType") or field.get("fieldType") or field.get("valueType")
                )
                description = (
                    field.get("description")
                    or field.get("label")
                    or field.get("help")
                    or field.get("hint")
                    or field.get("displayName")
                    or field.get("summary")
                    or ""
                )

                is_nullable = None
                if "mandatory" in field:
                    is_nullable = not bool(field.get("mandatory"))
                elif "isMandatory" in field:
                    is_nullable = not bool(field.get("isMandatory"))
                elif "required" in field:
                    is_nullable = not bool(field.get("required"))
                elif "nullable" in field:
                    is_nullable = bool(field.get("nullable"))

                fields_metadata.append(
                    {
                        "table_name": self.record_type,
                        "column_name": name,
                        "data_type": self._normalize_metadata_type(data_type),
                        "column_description": description,
                        "is_nullable": is_nullable,
                        "column_default": field.get("defaultValue") or field.get("default") or "",
                    }
                )
            if fields_metadata:
                return fields_metadata

        properties = metadata.get("properties")
        required = set(metadata.get("required") or [])
        if isinstance(properties, dict):
            for name, info in properties.items():
                if not isinstance(info, dict):
                    info = {}
                data_type = info.get("type") or info.get("format") or info.get("$ref")
                description = (
                    info.get("description")
                    or info.get("title")
                    or info.get("label")
                    or info.get("displayName")
                    or info.get("summary")
                    or ""
                )
                if "nullable" in info:
                    is_nullable = bool(info.get("nullable"))
                elif required:
                    is_nullable = name not in required
                else:
                    is_nullable = None
                fields_metadata.append(
                    {
                        "table_name": self.record_type,
                        "column_name": name,
                        "data_type": self._normalize_metadata_type(data_type),
                        "column_description": description,
                        "is_nullable": is_nullable,
                        "column_default": info.get("default") or "",
                    }
                )

        return fields_metadata

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

        if (
            hasattr(self.handler, "_unsupported_record_types")
            and self.record_type in self.handler._unsupported_record_types
        ):
            if targets:
                return pd.DataFrame(columns=targets)
            return pd.DataFrame(columns=["internalId"])

        # If filtering on a specific id/internalId, fetch the full record directly.
        record_id = None
        record_id_cond = None
        for cond in conditions or []:
            if cond.op == FilterOperator.EQUAL and cond.column.lower() in ("id", "internalid"):
                record_id = cond.value
                record_id_cond = cond
                break
        if record_id is not None:
            record_url = f"{self._base_path}/{record_id}"
            try:
                payload = self.handler._request("GET", record_url)
            except RuntimeError as exc:
                if self._should_skip_record_type(exc):
                    self._mark_record_type_unsupported()
                    if targets:
                        return pd.DataFrame(columns=targets)
                    return pd.DataFrame(columns=["internalId"])
                if targets:
                    return pd.DataFrame(columns=targets)
                return pd.DataFrame(columns=["internalId"])
            df = pd.DataFrame([payload]) if isinstance(payload, dict) else pd.DataFrame()
            if targets:
                df = df.reindex(columns=targets) if not df.empty else pd.DataFrame(columns=targets)
            else:
                df = df if not df.empty else pd.DataFrame(columns=["internalId"])

            remaining_conditions = [
                cond for cond in conditions or [] if cond.column.lower() not in ("id", "internalid")
            ]
            if remaining_conditions:
                filters = [[cond.op.value, cond.column, cond.value] for cond in remaining_conditions]
                df = filter_dataframe(df, filters)

            if record_id_cond is not None:
                record_id_cond.applied = True
            for cond in remaining_conditions:
                cond.applied = True

            return df

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
                if self._should_skip_record_type(exc):
                    self._mark_record_type_unsupported()
                    if targets:
                        return pd.DataFrame(columns=targets)
                    return pd.DataFrame(columns=["internalId"])
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

            if remaining is not None:
                if len(page_items) > remaining:
                    page_items = page_items[:remaining]
                remaining -= len(page_items)

            items.extend(page_items)

            if remaining is not None and remaining <= 0:
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

    @staticmethod
    def _should_skip_record_type(exc: RuntimeError) -> bool:
        message = str(exc).lower()
        if "record" in message and "was not found" in message:
            return True
        if "invalid method" in message or "method not allowed" in message:
            return True
        if "operation is not allowed" in message:
            return True
        return False

    def _mark_record_type_unsupported(self) -> None:
        if hasattr(self.handler, "_unsupported_record_types"):
            self.handler._unsupported_record_types.add(str(self.record_type).lower())

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

        failures = []
        for record_id in record_ids:
            path = f"{self._base_path}/{record_id}"
            try:
                self.handler._request("PATCH", path, json=values)
            except RuntimeError as exc:
                failures.append((record_id, str(exc)))

        if failures:
            details = "; ".join([f"{record_id}: {message}" for record_id, message in failures])
            raise RuntimeError(f"Failed to update {len(failures)} record(s): {details}")

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
        columns_metadata = self.meta_get_columns()
        if columns_metadata:
            return [column.get("column_name") for column in columns_metadata if column.get("column_name")]

        sample = self.list(limit=1)
        if sample.empty:
            return []
        return list(sample.columns)

    def meta_get_tables(self, table_name: str, main_metadata=None) -> dict:
        """
        Retrieves table metadata for the NetSuite record type.

        Args:
            table_name (str): The table name for the record type.
            main_metadata: Unused; present for interface compatibility.

        Returns:
            dict: Table metadata including table_name, table_type, table_description, and row_count.
        """
        return {
            "table_name": table_name,
            "table_type": "BASE TABLE",
            "table_description": "",
            "row_count": None,
        }

    def meta_get_columns(self, table_name: str = None, **kwargs) -> List[dict]:
        """
        Retrieves column metadata for the NetSuite record type.

        Args:
            table_name (str): Optional table name override.
            **kwargs: Additional handler-specific arguments.

        Returns:
            List[dict]: Column metadata entries with table_name, column_name, data_type,
            column_description, is_nullable, and column_default.
        """
        metadata = self._extract_field_metadata()
        if metadata:
            return metadata

        sample_record = self._get_sample_record()
        if not sample_record:
            return []

        columns = []
        for column_name, value in sample_record.items():
            columns.append(
                {
                    "table_name": self.record_type,
                    "column_name": column_name,
                    "data_type": self._infer_column_type(value),
                    "column_description": "",
                    "is_nullable": None,
                    "column_default": "",
                }
            )
        return columns

    def _get_sample_record(self) -> dict:
        """
        Retrieves a sample record for column inference when metadata is unavailable.

        Returns:
            dict: A sample record payload, or an empty dict if unavailable.
        """
        try:
            sample = self.list(limit=1)
        except RuntimeError as exc:
            if self._should_skip_record_type(exc):
                self._mark_record_type_unsupported()
            return {}
        if sample.empty:
            return {}

        record = sample.iloc[0].to_dict()
        if self._is_minimal_record(record):
            record = self._expand_minimal_record(record)

        return record if isinstance(record, dict) else {}

    def _is_minimal_record(self, record: dict) -> bool:
        if not isinstance(record, dict) or not record:
            return False
        keys = {str(key).lower() for key in record.keys()}
        allowed = {"id", "internalid", "links"}
        return keys.issubset(allowed)

    def _expand_minimal_record(self, record: dict) -> dict:
        """
        Expands a minimal record (id/internalId/links only) into a full record.

        Args:
            record (dict): The minimal record payload.

        Returns:
            dict: The full record payload when available, otherwise the original record.
        """
        if not isinstance(record, dict):
            return record

        links = record.get("links") or []
        if isinstance(links, list):
            for link in links:
                if not isinstance(link, dict):
                    continue
                if link.get("rel") == "self" and link.get("href"):
                    try:
                        payload = self.handler._request("GET", link.get("href"))
                        return payload if isinstance(payload, dict) else record
                    except RuntimeError:
                        return record

        record_id = record.get("internalId") or record.get("id")
        if record_id is None:
            return record

        try:
            payload = self.handler._request("GET", f"{self._base_path}/{record_id}")
            return payload if isinstance(payload, dict) else record
        except RuntimeError:
            return record

    @staticmethod
    def _infer_column_type(value) -> str:
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, (dict, list)):
            return "json"
        if value is None:
            return "str"
        return "str"

    def meta_get_primary_keys(self, table_name: str) -> List[dict]:
        """
        Retrieves primary key metadata for the NetSuite record type.

        Args:
            table_name (str): The table name for the record type.

        Returns:
            List[dict]: Primary key metadata entries with table_name and column_name.
        """
        columns = {col.get("column_name") for col in self.meta_get_columns() if col.get("column_name")}

        primary_key = None
        for candidate in ("internalId", "id", "Id"):
            if candidate in columns:
                primary_key = candidate
                break

        if not primary_key:
            return []

        return [{"table_name": table_name, "column_name": primary_key}]

    def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[dict]:
        """
        Retrieves foreign key metadata inferred from record reference fields.

        Args:
            table_name (str): The table name for the record type.
            all_tables (List[str]): All available table names for relationship resolution.

        Returns:
            List[dict]: Foreign key metadata entries with parent/child table and column names.
        """
        metadata = self._get_resource_metadata()
        if not isinstance(metadata, dict):
            return []

        all_tables_lower = {table.lower() for table in all_tables}
        parent_candidates = []

        fields = metadata.get("fields")
        if isinstance(fields, list):
            for field in fields:
                if not isinstance(field, dict):
                    continue
                field_type = field.get("type") or field.get("fieldType")
                if str(field_type).lower() not in ("recordref", "recordreference", "reference"):
                    continue
                target = (
                    field.get("recordType")
                    or field.get("referenceType")
                    or field.get("referenceRecordType")
                    or field.get("targetRecordType")
                    or field.get("refType")
                )
                if not target or str(target).lower() not in all_tables_lower:
                    continue
                parent_candidates.append((str(target), field.get("id") or field.get("name")))

        properties = metadata.get("properties")
        if isinstance(properties, dict):
            for name, info in properties.items():
                if not isinstance(info, dict):
                    continue
                info_type = info.get("type") or info.get("format")
                if str(info_type).lower() not in ("recordref", "recordreference", "reference"):
                    continue
                target = (
                    info.get("recordType")
                    or info.get("referenceType")
                    or info.get("referenceRecordType")
                    or info.get("targetRecordType")
                    or info.get("$ref")
                )
                if isinstance(target, str) and "/" in target:
                    target = target.split("/")[-1]
                if not target or str(target).lower() not in all_tables_lower:
                    continue
                parent_candidates.append((str(target), name))

        if not parent_candidates:
            return []

        columns = {col.get("column_name") for col in self.meta_get_columns() if col.get("column_name")}
        parent_key = "internalId" if "internalId" in columns else ("id" if "id" in columns else None)

        foreign_keys = []
        for parent_table, column_name in parent_candidates:
            if not column_name:
                continue
            foreign_keys.append(
                {
                    "parent_table_name": parent_table,
                    "parent_column_name": parent_key or "id",
                    "child_table_name": table_name,
                    "child_column_name": column_name,
                }
            )

        return foreign_keys
