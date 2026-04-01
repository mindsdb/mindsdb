from __future__ import annotations

from typing import Any

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator


def _normalize_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


class SentryProjectsTable(APIResource):
    name = "projects"

    def list(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort=None,
        targets: list[str] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        if conditions:
            raise ValueError("Sentry projects does not support WHERE filters in V1")

        client = self.handler.connect()
        projects = client.list_projects(limit=limit)
        rows = [self._flatten_project(project) for project in projects]
        return pd.DataFrame(rows, columns=self.get_columns())

    @staticmethod
    def _flatten_project(project: dict[str, Any]) -> dict[str, Any]:
        latest_release = project.get("latestRelease")
        if isinstance(latest_release, dict):
            latest_release = latest_release.get("version")

        return {
            "project_id": _normalize_int(project.get("id")),
            "project_slug": project.get("slug"),
            "project_name": project.get("name"),
            "platform": project.get("platform"),
            "date_created": _normalize_timestamp(project.get("dateCreated")),
            "latest_release": latest_release,
        }

    @staticmethod
    def get_columns() -> list[str]:
        return [
            "project_id",
            "project_slug",
            "project_name",
            "platform",
            "date_created",
            "latest_release",
        ]


class SentryIssuesTable(APIResource):
    name = "issues"

    def list(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort=None,
        targets: list[str] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        client = self.handler.connect()
        query_string = self._build_query_string(conditions or [])
        resolved_project = client.resolve_project()

        effective_limit = 100 if limit is None else min(int(limit), 1000)
        fetch_limit = 1000 if sort else effective_limit

        issues = client.list_issues(
            project_id=resolved_project.get("id"),
            query=query_string,
            limit=fetch_limit,
        )
        rows = [self._flatten_issue(issue) for issue in issues]
        df = pd.DataFrame(rows, columns=self.get_columns())
        return self._apply_local_filters(df, conditions or [])

    @staticmethod
    def _build_query_string(conditions: list[FilterCondition]) -> str:
        query_value: str | None = None
        structured_filters: list[str] = []

        for condition in conditions:
            if condition.column == "query":
                if condition.op != FilterOperator.EQUAL:
                    raise ValueError("Unsupported where operation for query")
                query_value = str(condition.value)
                condition.applied = True
                continue

            if condition.column in {"status", "level"}:
                if condition.op != FilterOperator.EQUAL:
                    raise ValueError(f"Unsupported where operation for {condition.column}")
                structured_filters.append(f"{condition.column}:{condition.value}")
                condition.applied = True

        if query_value is not None and structured_filters:
            raise ValueError("Sentry issues query filter cannot be combined with status or level filters")

        if query_value is not None:
            return query_value

        return " ".join(structured_filters)

    @staticmethod
    def _apply_local_filters(df: pd.DataFrame, conditions: list[FilterCondition]) -> pd.DataFrame:
        if df.empty:
            return df

        for condition in conditions:
            if condition.applied:
                continue
            if condition.column not in {"first_seen", "last_seen"}:
                continue
            if condition.column not in df.columns:
                continue

            series = pd.to_datetime(df[condition.column], utc=True, errors="coerce")
            value = pd.to_datetime(condition.value, utc=True, errors="coerce")
            if pd.isna(value):
                raise ValueError(f"Unsupported where value for {condition.column}: {condition.value}")

            if isinstance(condition.value, str) and len(condition.value) == 10 and condition.op == FilterOperator.EQUAL:
                mask = series.dt.strftime("%Y-%m-%d") == condition.value
            elif condition.op == FilterOperator.EQUAL:
                mask = series == value
            elif condition.op == FilterOperator.GREATER_THAN:
                mask = series > value
            elif condition.op == FilterOperator.GREATER_THAN_OR_EQUAL:
                mask = series >= value
            elif condition.op == FilterOperator.LESS_THAN:
                mask = series < value
            elif condition.op == FilterOperator.LESS_THAN_OR_EQUAL:
                mask = series <= value
            else:
                raise ValueError(f"Unsupported where operation for {condition.column}")

            df = df[mask.fillna(False)]
            condition.applied = True

        return df

    @staticmethod
    def _flatten_issue(issue: dict[str, Any]) -> dict[str, Any]:
        metadata = issue.get("metadata") or {}
        type_value = issue.get("type")
        if isinstance(type_value, dict):
            type_value = type_value.get("name")

        return {
            "issue_id": _normalize_int(issue.get("id")),
            "short_id": issue.get("shortId"),
            "title": issue.get("title") or metadata.get("title") or issue.get("culprit"),
            "type": type_value,
            "culprit": issue.get("culprit"),
            "status": issue.get("status"),
            "level": issue.get("level"),
            "count": _normalize_int(issue.get("count")),
            "user_count": _normalize_int(issue.get("userCount")),
            "first_seen": _normalize_timestamp(issue.get("firstSeen")),
            "last_seen": _normalize_timestamp(issue.get("lastSeen")),
            "permalink": issue.get("permalink"),
        }

    @staticmethod
    def get_columns() -> list[str]:
        return [
            "issue_id",
            "short_id",
            "title",
            "type",
            "culprit",
            "status",
            "level",
            "count",
            "user_count",
            "first_seen",
            "last_seen",
            "permalink",
        ]
