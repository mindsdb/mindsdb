"""Lightweight Jira client used by the AutoBankingCustomerService workflow."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence

from jira import JIRA, JIRAError


class JiraClientError(RuntimeError):
    """Raised when Jira API interaction fails."""


@dataclass
class JiraConfig:
    """Configuration required to interact with the Jira REST API."""

    base_url: str
    email: str
    api_token: str
    project_key: str
    issue_type: str = "Task"
    labels: Sequence[str] | None = None

    @classmethod
    def from_env(cls) -> Optional["JiraConfig"]:
        """Build a JiraConfig from environment variables."""
        base_url = os.getenv("JIRA_BASE_URL")
        email = os.getenv("JIRA_EMAIL")
        api_token = os.getenv("JIRA_API_TOKEN")
        project_key = os.getenv("JIRA_PROJECT_KEY")

        if not all([base_url, email, api_token, project_key]):
            return None

        issue_type = os.getenv("JIRA_ISSUE_TYPE", "Task")
        labels_env = os.getenv("JIRA_LABELS")
        labels = tuple(
            label.strip() for label in labels_env.split(",") if label.strip()
        ) if labels_env else None

        return cls(
            base_url=base_url.rstrip("/"),
            email=email,
            api_token=api_token,
            project_key=project_key,
            issue_type=issue_type,
            labels=labels,
        )


class JiraClient:
    """Wrapper around the jira-python SDK for creating issues."""

    def __init__(self, config: JiraConfig):
        self._config = config
        self._client = JIRA(
            options={"server": config.base_url},
            basic_auth=(config.email, config.api_token),
        )

    def create_issue(
        self,
        summary: str,
        description: str,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        """Create a Jira issue and return its key and browse URL."""
        fields: Dict[str, Any] = {
            "project": {"key": self._config.project_key},
            "summary": summary[:255],
            "issuetype": {"name": self._config.issue_type},
            "description": description,
        }

        if self._config.labels:
            fields["labels"] = list(self._config.labels)

        if extra_fields:
            fields.update(extra_fields)

        try:
            issue = self._client.create_issue(fields=fields)
        except JIRAError as exc:  # pragma: no cover - network/remote failures
            status = getattr(exc, "status_code", None)
            error_message = exc.text or getattr(exc, "message", None) or str(exc)
            prefix = f"Jira API error {status}" if status else "Jira API error"
            raise JiraClientError(f"{prefix}: {error_message}") from exc
        except Exception as exc:  # pragma: no cover - catch unexpected SDK errors
            raise JiraClientError(f"Unexpected Jira error: {exc}") from exc

        if not issue or not getattr(issue, "key", None):
            raise JiraClientError("Jira API response missing issue key")

        issue_key = issue.key
        try:
            issue_url = issue.permalink()
        except Exception:  # pragma: no cover - permalink may fail if browse URL disabled
            issue_url = f"{self._config.base_url}/browse/{issue_key}"

        return {"issue_key": issue_key, "issue_url": issue_url}


def build_default_client() -> Optional[JiraClient]:
    """Helper that instantiates a JiraClient from environment variables."""
    config = JiraConfig.from_env()
    if not config:
        return None
    return JiraClient(config)


__all__ = [
    "JiraClient",
    "JiraClientError",
    "JiraConfig",
    "build_default_client",
]
