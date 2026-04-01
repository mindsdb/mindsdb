from __future__ import annotations

import json
from typing import Any

import pandas as pd

from mindsdb.integrations.handlers.sentry_handler.explore.sql import (
    LOG_TABLE_COLUMNS,
    TIMESERIES_COLUMNS,
    build_logs_request,
    build_logs_timeseries_request,
)
from mindsdb.integrations.libs.api_handler import APIResource


def _normalize_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return str(value)
    return timestamp.to_pydatetime().isoformat()


def _serialize_extra_payload(row: dict[str, Any]) -> str | None:
    known_keys = {
        "timestamp",
        "severity",
        "level",
        "message",
        "body",
        "trace.id",
        "trace_id",
        "span.id",
        "span_id",
        "sentry.release",
        "release",
        "logger.name",
        "logger",
    }
    payload: dict[str, Any] = {}

    attributes = row.get("attributes")
    if isinstance(attributes, dict):
        payload.update(attributes)
    elif attributes is not None:
        payload["attributes"] = attributes

    extra = row.get("extra")
    if isinstance(extra, dict):
        payload.update(extra)
    elif extra is not None:
        payload["extra"] = extra

    for key, value in row.items():
        if key in known_keys or key in {"attributes", "extra"}:
            continue
        payload[key] = value

    if not payload:
        return None

    return json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str)


class SentryLogsTable(APIResource):
    name = "logs"

    def list(
        self,
        conditions=None,
        limit: int | None = None,
        sort=None,
        targets: list[str] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        client = self.handler.connect()
        project = client.sentry_client.resolve_project()
        request = build_logs_request(
            project_id=int(project["id"]),
            environment=self.handler.environment,
            conditions=conditions or [],
            limit=limit,
            sort=sort,
            targets=targets,
        )
        rows = client.query_table(request)
        flattened_rows = [
            self._flatten_row(row, project_slug=project.get("slug"), project_id=project.get("id"))
            for row in rows
        ]
        return pd.DataFrame(flattened_rows, columns=self.get_columns())

    def _flatten_row(self, row: dict[str, Any], *, project_slug: str | None, project_id: Any) -> dict[str, Any]:
        return {
            "timestamp": _normalize_timestamp(row.get("timestamp")),
            "level": row.get("severity") or row.get("level"),
            "message": row.get("message") or row.get("body"),
            "trace_id": row.get("trace.id") or row.get("trace_id"),
            "span_id": row.get("span.id") or row.get("span_id"),
            "release": row.get("sentry.release") or row.get("release"),
            "environment": self.handler.environment,
            "project_id": int(project_id) if project_id is not None else None,
            "project_slug": project_slug,
            "logger": row.get("logger.name") or row.get("logger"),
            "extra_json": _serialize_extra_payload(row),
        }

    @staticmethod
    def get_columns() -> list[str]:
        return LOG_TABLE_COLUMNS


class SentryLogsTimeseriesTable(APIResource):
    name = "logs_timeseries"

    def list(
        self,
        conditions=None,
        limit: int | None = None,
        sort=None,
        targets: list[str] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        client = self.handler.connect()
        project = client.sentry_client.resolve_project()
        request = build_logs_timeseries_request(
            project_id=int(project["id"]),
            environment=self.handler.environment,
            conditions=conditions or [],
        )
        series = client.query_timeseries(request)

        rows: list[dict[str, Any]] = []
        for item in series:
            for value in item.get("values") or []:
                rows.append(
                    {
                        "bucket_start": _normalize_timestamp(
                            pd.to_datetime(value.get("timestamp"), unit="ms", utc=True, errors="coerce")
                        ),
                        "value": value.get("value"),
                    }
                )

        df = pd.DataFrame(rows, columns=self.get_columns())
        if limit is not None and len(df) > int(limit):
            df = df.head(int(limit))
        return df

    @staticmethod
    def get_columns() -> list[str]:
        return TIMESERIES_COLUMNS
