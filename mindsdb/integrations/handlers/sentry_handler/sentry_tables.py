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
        return pd.DataFrame(rows, columns=self.get_columns())

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
                continue

            raise ValueError(f"Unsupported where argument {condition.column}")

        if query_value is not None and structured_filters:
            raise ValueError("Sentry issues query filter cannot be combined with status or level filters")

        if query_value is not None:
            return query_value

        return " ".join(structured_filters)

    @staticmethod
    def _flatten_issue(issue: dict[str, Any]) -> dict[str, Any]:
        metadata = issue.get("metadata") or {}
        type_value = issue.get("type")
        if isinstance(type_value, dict):
            type_value = type_value.get("name")

        environment = issue.get("environment")
        if environment is None:
            environments = issue.get("environments")
            if isinstance(environments, list) and len(environments) == 1:
                environment = environments[0]

        return {
            "issue_id": _normalize_int(issue.get("id")),
            "short_id": issue.get("shortId"),
            "title": issue.get("title") or metadata.get("title") or issue.get("culprit"),
            "type": type_value,
            "culprit": issue.get("culprit"),
            "status": issue.get("status"),
            "level": issue.get("level"),
            "environment": environment,
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
            "environment",
            "count",
            "user_count",
            "first_seen",
            "last_seen",
            "permalink",
        ]
