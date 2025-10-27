"""Salesforce client for creating cases from banking conversations."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from simple_salesforce import Salesforce


class SalesforceClientError(RuntimeError):
    """Raised when Salesforce API interaction fails."""


@dataclass
class SalesforceConfig:
    """Configuration required to interact with the Salesforce API."""

    username: str
    password: str
    security_token: str
    domain: str

    @classmethod
    def from_env(cls) -> Optional["SalesforceConfig"]:
        """Build a SalesforceConfig from environment variables."""
        username = os.getenv("SALESFORCE_USERNAME")
        password = os.getenv("SALESFORCE_PASSWORD")
        security_token = os.getenv("SALESFORCE_SECURITY_TOKEN")
        domain = os.getenv("SALESFORCE_DOMAIN", "login")

        if not all([username, password, security_token]):
            return None

        return cls(
            username=username,
            password=password,
            security_token=security_token,
            domain=domain,
        )


class SalesforceClient:
    """Wrapper around the simple-salesforce SDK for creating cases."""

    def __init__(self, config: SalesforceConfig):
        self._config = config
        try:
            # Try different connection methods for Developer Edition
            if config.domain and 'dev-ed' in config.domain:
                # For Developer Edition, try without domain first
                try:
                    self._client = Salesforce(
                        username=config.username,
                        password=config.password,
                        security_token=config.security_token,
                    )
                except Exception:
                    # If that fails, try with domain
                    self._client = Salesforce(
                        username=config.username,
                        password=config.password,
                        security_token=config.security_token,
                        domain=config.domain,
                    )
            else:
                # Standard connection
                self._client = Salesforce(
                    username=config.username,
                    password=config.password,
                    security_token=config.security_token,
                    domain=config.domain,
                )
        except Exception as exc:
            raise SalesforceClientError(f"Failed to connect to Salesforce: {exc}")

    def create_case(
        self,
        conversation_id: str,
        summary: str,
        status: str,
        priority: str = "Medium",
        origin: str = "AI Workflow",
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        """Create a Salesforce case and return its ID and URL."""
        
        # Create subject from conversation_id and status
        subject = f"Banking Customer Service - {conversation_id[:8]}... ({status})"
        
        # Create description with conversation details
        description = f"""Auto-generated case from AI-powered banking customer service workflow.

Conversation ID: {conversation_id}
Status: {status}
AI Summary: {summary}

This case was automatically created by our AI workflow that analyzes customer conversations and creates cases for tracking and follow-up."""
        
        fields: Dict[str, Any] = {
            "Subject": subject[:255],  # Salesforce has a 255 char limit
            "Description": description,
            "Priority": priority,
            "Status": "New" if status == "UNRESOLVED" else "Closed",
            "Origin": origin,
        }

        # Only add Type and Reason if they exist in your Salesforce org
        # Commenting out to avoid field errors
        # "Type": "Customer Service",
        # "Reason": "Customer Inquiry" if status == "RESOLVED" else "Customer Issue",

        if extra_fields:
            fields.update(extra_fields)

        try:
            result = self._client.Case.create(fields)
        except Exception as exc:
            raise SalesforceClientError(f"Failed to create Salesforce case: {exc}") from exc

        if not result or not result.get("id"):
            raise SalesforceClientError("Salesforce API response missing case ID")

        case_id = result["id"]
        # Build the case URL based on domain
        if self._config.domain == "test":
            base_url = "https://test.salesforce.com"
        else:
            base_url = "https://login.salesforce.com"
        case_url = f"{base_url}/{case_id}"

        return {"case_id": case_id, "case_url": case_url}

    def get_case(self, case_id: str) -> Dict[str, Any]:
        """Retrieve a Salesforce case by ID."""
        try:
            result = self._client.Case.get(case_id)
            return result
        except Exception as exc:
            raise SalesforceClientError(f"Failed to retrieve Salesforce case {case_id}: {exc}") from exc


def build_default_client() -> Optional[SalesforceClient]:
    """Helper that instantiates a SalesforceClient from environment variables."""
    config = SalesforceConfig.from_env()
    if not config:
        return None
    return SalesforceClient(config)


__all__ = [
    "SalesforceClient",
    "SalesforceClientError",
    "SalesforceConfig",
    "build_default_client",
]