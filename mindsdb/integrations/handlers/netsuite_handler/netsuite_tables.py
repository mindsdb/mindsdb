from typing import Any, List, Optional

import json
import pandas as pd

from mindsdb.integrations.libs.api_handler import MetaAPIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator
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
        self._column_metadata_cache = None
        super().__init__(handler, table_name=record_type)

    def _get_resource_metadata(self) -> List[dict]:
        """
        Retrieves SuiteQL column metadata for this NetSuite record type.

        Returns:
            List[dict]: Column metadata entries when available.
        """
        if self._column_metadata_cache is not None:
            return self._column_metadata_cache

        try:
            payload = self.handler._suiteql_select(table=self.record_type, limit=1)
        except RuntimeError as exc:
            if self._should_skip_record_type(exc):
                self._mark_record_type_unsupported()
            self._column_metadata_cache = []
            return self._column_metadata_cache

        self._column_metadata_cache = self._payload_to_column_metadata(payload)
        return self._column_metadata_cache

    def _payload_to_column_metadata(self, payload: Any) -> List[dict]:
        """
        Converts SuiteQL payload column metadata into a normalized list.
        """
        if not isinstance(payload, dict):
            return []

        columns: List[dict] = []
        metadata = payload.get("columnMetadata") or payload.get("columns") or []
        if isinstance(metadata, list) and metadata:
            for idx, col in enumerate(metadata):
                if isinstance(col, dict):
                    name = col.get("name") or col.get("label") or col.get("id") or f"col_{idx}"
                    data_type = col.get("type") or col.get("dataType") or col.get("sqlType") or col.get("fieldType")
                    description = col.get("description") or col.get("label") or ""
                    is_nullable = col.get("nullable") if "nullable" in col else None
                    default_value = col.get("defaultValue") or col.get("default") or ""
                else:
                    name = str(col)
                    data_type = None
                    description = ""
                    is_nullable = None
                    default_value = ""
                columns.append(
                    {
                        "column_name": name,
                        "data_type": self._normalize_metadata_type(data_type),
                        "column_description": description,
                        "is_nullable": is_nullable,
                        "column_default": default_value,
                    }
                )

        if columns:
            return columns

        items = payload.get("items") or []
        if isinstance(items, list) and items and isinstance(items[0], dict):
            for name, value in items[0].items():
                columns.append(
                    {
                        "column_name": name,
                        "data_type": self._infer_column_type(value),
                        "column_description": "",
                        "is_nullable": None,
                        "column_default": "",
                    }
                )

        return columns

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
        Extracts column metadata from SuiteQL responses.

        Returns:
            List[dict]: Column metadata entries with table_name, column_name, data_type,
            column_description, is_nullable, and column_default.
        """
        metadata = self._get_resource_metadata()
        if not metadata:
            return []

        fields_metadata = []
        for field in metadata:
            fields_metadata.append(
                {
                    "table_name": self.record_type,
                    "column_name": field.get("column_name"),
                    "data_type": field.get("data_type"),
                    "column_description": field.get("column_description"),
                    "is_nullable": field.get("is_nullable"),
                    "column_default": field.get("column_default"),
                }
            )

        return fields_metadata

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: Optional[int] = None,
        sort: Optional[list] = None,
        targets: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Fetches records using SuiteQL.
        """
        limit = int(limit) if limit is not None else None

        # Guard for tables we already marked unsupported
        if (
            hasattr(self.handler, "_unsupported_record_types")
            and self.record_type in self.handler._unsupported_record_types
        ):
            if targets:
                return pd.DataFrame(columns=targets)
            return pd.DataFrame(columns=["id"])

        def _sql_quote(value) -> str:
            if value is None:
                return "NULL"
            if isinstance(value, bool):
                # NetSuite often uses 'T'/'F' in some contexts, but SuiteQL generally accepts TRUE/FALSE too.
                # We'll use string T/F to be safe with record fields.
                return "'T'" if value else "'F'"
            if isinstance(value, (int, float)):
                return str(value)
            s = str(value).replace("'", "''")
            return f"'{s}'"

        def _normalize_col(col: str) -> str:
            # Your users might filter by internalId; SuiteQL base tables typically expose 'id'
            if col.lower() in ("internalid",):
                return "id"
            return col

        # Build WHERE (push down only EQUAL for now – predictable)
        where_parts = []
        for cond in conditions or []:
            if cond.op == FilterOperator.EQUAL:
                col = _normalize_col(cond.column)
                if cond.value is None:
                    where_parts.append(f"{col} IS NULL")
                else:
                    where_parts.append(f"{col} = {_sql_quote(cond.value)}")
                cond.applied = True

        where_sql = ""
        if where_parts:
            where_sql = " WHERE " + " AND ".join(where_parts)

        # ORDER BY
        order_by_sql = ""
        if sort:
            parts = []
            for s in sort:
                direction = "ASC" if s.ascending else "DESC"
                parts.append(f"{s.column} {direction}")
            if parts:
                order_by_sql = " ORDER BY " + ", ".join(parts)

        # Execute SuiteQL
        payload = self.handler._suiteql_select(
            table=self.record_type,
            where_sql=where_sql,
            limit=limit,
            targets=targets,
            order_by_sql=order_by_sql,
        )

        df = self._payload_to_dataframe(payload, targets=targets)

        if df.empty:
            if targets:
                return pd.DataFrame(columns=targets)
            return pd.DataFrame(columns=["id"])

        # Pretty / stable cell parsing
        df = self._prettify_dataframe(df)

        if targets:
            df = df.reindex(columns=targets)

        return df

    def _payload_to_dataframe(self, payload: Any, targets: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Convert SuiteQL response payload into a DataFrame.
        """
        if not isinstance(payload, dict):
            return pd.DataFrame(columns=targets or [])

        items = payload.get("items") or []
        if not isinstance(items, list) or not items:
            return pd.DataFrame(columns=targets or [])

        first = items[0]

        if isinstance(first, dict) and "values" in first:
            col_meta = payload.get("columnMetadata") or payload.get("columns") or []
            cols: List[str] = []
            for idx, c in enumerate(col_meta):
                if isinstance(c, dict):
                    cols.append(c.get("name") or c.get("label") or f"col_{idx}")
                else:
                    cols.append(str(c))

            rows = []
            for it in items:
                if isinstance(it, dict):
                    rows.append(it.get("values") or [])
                else:
                    rows.append([])

            max_len = max((len(r) for r in rows), default=0)
            if len(cols) < max_len:
                cols.extend([f"col_{i}" for i in range(len(cols), max_len)])

            padded = []
            for r in rows:
                r = list(r)
                if len(r) < len(cols):
                    r = r + [None] * (len(cols) - len(r))
                else:
                    r = r[: len(cols)]
                padded.append(r)

            seen = {}
            deduped = []
            for name in cols:
                count = seen.get(name, 0) + 1
                seen[name] = count
                deduped.append(name if count == 1 else f"{name}_{count}")

            return pd.DataFrame(padded, columns=deduped)

        if isinstance(first, dict):
            return pd.DataFrame(items)

        return pd.DataFrame(columns=targets or [])

    def _prettify_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Make SuiteQL/REST-ish nested values readable
        """

        def pick_href(obj: Any) -> Optional[str]:
            if isinstance(obj, dict):
                href = obj.get("href")
                if href:
                    return str(href)
                links = obj.get("links")
                if isinstance(links, list):
                    for link in links:
                        if isinstance(link, dict) and link.get("rel") == "self" and link.get("href"):
                            return str(link.get("href"))
            if isinstance(obj, list):
                for link in obj:
                    if isinstance(link, dict) and link.get("rel") == "self" and link.get("href"):
                        return str(link.get("href"))
            return None

        def normalize_cell(v: Any) -> Any:
            if v is None:
                return None

            # Most common NetSuite ref objects
            if isinstance(v, dict):
                # prefer refName for readability
                if "refName" in v and v.get("refName") is not None:
                    return v.get("refName")
                if "name" in v and v.get("name") is not None:
                    return v.get("name")
                if "id" in v and v.get("id") is not None and len(v.keys()) <= 3:
                    # small dict with id-ish fields
                    return v.get("id")
                href = pick_href(v)
                if href:
                    return href
                # fallback to stable JSON
                try:
                    return json.dumps(v, ensure_ascii=False, sort_keys=True)
                except Exception:
                    return str(v)

            if isinstance(v, list):
                href = pick_href(v)
                if href:
                    return href
                try:
                    return json.dumps(v, ensure_ascii=False, sort_keys=True)
                except Exception:
                    return str(v)

            return v

        out = df.copy()
        for col in out.columns:
            out[col] = out[col].map(normalize_cell)
        return out

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
        raise NotImplementedError("NetSuite handler is read-only via SuiteQL.")

    def modify(self, conditions: List[FilterCondition], values: dict):
        """
        Updates records in NetSuite identified by id/internalId.

        Args:
            conditions (List[FilterCondition]): Conditions to select records.
            values (dict): Updated values.
        """
        raise NotImplementedError("NetSuite handler is read-only via SuiteQL.")

    def remove(self, conditions: List[FilterCondition]):
        """
        Deletes records in NetSuite identified by id/internalId.

        Args:
            conditions (List[FilterCondition]): Conditions to select records.
        """
        raise NotImplementedError("NetSuite handler is read-only via SuiteQL.")

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

        record_id = record.get("internalId") or record.get("id")
        if record_id is None:
            return record

        try:

            def _sql_quote(value) -> str:
                if value is None:
                    return "NULL"
                if isinstance(value, bool):
                    return "'T'" if value else "'F'"
                if isinstance(value, (int, float)):
                    return str(value)
                s = str(value).replace("'", "''")
                return f"'{s}'"

            where_sql = f" WHERE id = {_sql_quote(record_id)}"
            payload = self.handler._suiteql_select(table=self.record_type, where_sql=where_sql, limit=1)
            df = self._payload_to_dataframe(payload)
            if df.empty:
                return record
            return df.iloc[0].to_dict()
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
        return []
