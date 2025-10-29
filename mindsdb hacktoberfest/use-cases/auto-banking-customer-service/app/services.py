"""Business logic for processing banking customer conversations."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import HTTPException

from . import db
from .mindsdb import get_agent, query_classification_agent, query_recommendation_agent
from .jira_client import JiraClient, JiraClientError
from .salesforce_client import SalesforceClient, SalesforceClientError

# Legacy support - keep for backward compatibility
classification_agent = None
_mindsdb_server = None
_db_config_override: Optional[Dict[str, Any]] = None
_jira_client: Optional[JiraClient] = None
_salesforce_client: Optional[SalesforceClient] = None


def set_agent(agent: Any) -> None:
    global classification_agent
    classification_agent = agent


def set_mindsdb_server(server: Any) -> None:
    global _mindsdb_server
    _mindsdb_server = server


def set_db_config(config: Dict[str, Any]) -> None:
    global _db_config_override
    _db_config_override = config


def set_jira_client(client: Optional[JiraClient]) -> None:
    """Lifecycle hook used during app startup to inject the Jira client."""
    global _jira_client
    _jira_client = client


def set_salesforce_client(client: Optional[SalesforceClient]) -> None:
    """Lifecycle hook used during app startup to inject the Salesforce client."""
    global _salesforce_client
    _salesforce_client = client


def get_db_config() -> Optional[Dict[str, Any]]:
    return _db_config_override


def clear_state() -> None:
    global classification_agent, _mindsdb_server, _jira_client, _salesforce_client
    classification_agent = None
    _mindsdb_server = None
    _jira_client = None
    _salesforce_client = None


def query_agent(conversation_text: str) -> Dict[str, Any]:
    """Query classification agent to analyze a conversation.

    This function uses the new agent registry system but falls back to
    the legacy global agent variable for backward compatibility.
    """
    # Try new agent registry first
    agent = get_agent("classification_agent")

    # Fall back to legacy global variable
    if agent is None:
        agent = classification_agent

    if agent is None:
        raise HTTPException(
            status_code=503,
            detail="Classification agent not initialized. Make sure MindsDB is running.",
        )

    try:
        return query_classification_agent(agent, conversation_text)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def process_conversation(conversation_text: str) -> Dict[str, Any]:
    analysis = query_agent(conversation_text)
    conversation_id = str(uuid.uuid4())

    timestamp = datetime.now().isoformat()
    display_text = (
        conversation_text[:500] + "..." if len(conversation_text) > 500 else conversation_text
    )

    jira_issue_key: Optional[str] = None
    jira_issue_url: Optional[str] = None
    jira_error: Optional[str] = None
    salesforce_case_id: Optional[str] = None
    salesforce_case_url: Optional[str] = None
    salesforce_error: Optional[str] = None
    recommendation: Optional[str] = None
    recommendation_error: Optional[str] = None

    # Create Salesforce case for all conversations (resolved and unresolved)
    if _salesforce_client is None:
        salesforce_error = "Salesforce client not configured; skipped case creation."
    else:
        try:
            status = "RESOLVED" if analysis["resolved"] else "UNRESOLVED"
            case = _salesforce_client.create_case(
                conversation_id=conversation_id,
                summary=analysis["summary"] or "No summary generated",
                status=status,
                priority="High" if not analysis["resolved"] else "Medium",
                origin="AI Workflow",
            )
            salesforce_case_id = case.get("case_id")
            salesforce_case_url = case.get("case_url")
        except SalesforceClientError as exc:
            salesforce_error = f"Failed to create Salesforce case: {exc}"
        except Exception as exc:  # pragma: no cover - guard against unexpected errors
            salesforce_error = f"Unexpected Salesforce error: {exc}"

    # Get recommendation for unresolved issues
    if not analysis["resolved"]:
        # Get recommendation agent from registry
        recommendation_agent = get_agent("recommendation_agent")

        if recommendation_agent is None:
            recommendation_error = "Recommendation agent not configured; skipped recommendation generation."
        else:
            try:
                recommendation = query_recommendation_agent(
                    recommendation_agent,
                    conversation_summary=analysis["summary"] or "No summary generated",
                    conversation_text=conversation_text
                )
            except ValueError as exc:
                recommendation_error = f"Recommendation agent not available: {exc}"
            except RuntimeError as exc:
                recommendation_error = f"Failed to get recommendation: {exc}"
            except Exception as exc:  # pragma: no cover - guard against unexpected errors
                recommendation_error = f"Unexpected recommendation error: {exc}"

    # Create Jira ticket only for unresolved issues
    if not analysis["resolved"]:
        if _jira_client is None:
            jira_error = "Jira client not configured; skipped ticket creation."
        else:
            try:
                summary = analysis["summary"][:255] if analysis["summary"] else "Unresolved customer conversation"
                description = _format_jira_description(
                    conversation_id=conversation_id,
                    conversation_text=conversation_text,
                    analysis=analysis,
                    created_at=timestamp,
                    recommendation=recommendation,
                )
                issue = _jira_client.create_issue(
                    summary=summary,
                    description=description,
                    extra_fields=None,
                )
                jira_issue_key = issue.get("issue_key")
                jira_issue_url = issue.get("issue_url")
            except JiraClientError as exc:
                jira_error = f"Failed to create Jira issue: {exc}"
            except Exception as exc:  # pragma: no cover - guard against unexpected errors
                jira_error = f"Unexpected Jira error: {exc}"

    db.insert_conversation_with_analysis(
        conversation_id=conversation_id,
        conversation_text=conversation_text,
        summary=analysis["summary"],
        resolved=analysis["resolved"],
        jira_issue_key=jira_issue_key,
        jira_issue_url=jira_issue_url,
        jira_issue_error=jira_error,
        salesforce_case_id=salesforce_case_id,
        salesforce_case_url=salesforce_case_url,
        salesforce_error=salesforce_error,
        recommendation=recommendation,
        recommendation_error=recommendation_error,
        db_config=_db_config_override,
    )

    return {
        "conversation_id": conversation_id,
        "conversation_text": display_text,
        "summary": analysis["summary"],
        "status": "RESOLVED" if analysis["resolved"] else "UNRESOLVED",
        "created_at": timestamp,
        "processed_at": timestamp,
        "jira_issue_key": jira_issue_key,
        "jira_issue_url": jira_issue_url,
        "jira_issue_error": jira_error,
        "salesforce_case_id": salesforce_case_id,
        "salesforce_case_url": salesforce_case_url,
        "salesforce_error": salesforce_error,
        "recommendation": recommendation,
        "recommendation_error": recommendation_error,
    }


def process_conversation_batch(
    conversation_texts: Iterable[str],
) -> Tuple[List[Dict[str, Any]], int]:
    cases: List[Dict[str, Any]] = []
    processed_count = 0

    for conv_text in conversation_texts:
        if not conv_text or not conv_text.strip():
            continue

        try:
            case_payload = process_conversation(conv_text)
        except Exception as exc:  # pragma: no cover - keep behaviour consistent
            print(f"Error processing conversation: {exc}")
            continue

        cases.append(case_payload)
        processed_count += 1

    return cases, processed_count


def _format_jira_description(
    *,
    conversation_id: str,
    conversation_text: str,
    analysis: Dict[str, Any],
    created_at: str,
    recommendation: Optional[str] = None,
) -> str:
    """Build a human-friendly Jira description payload."""
    resolved_status = "RESOLVED" if analysis.get("resolved") else "UNRESOLVED"
    summary = analysis.get("summary") or "No summary generated."
    preview = conversation_text if len(conversation_text) <= 2000 else f"{conversation_text[:2000]}..."

    description = (
        "Auto-generated from AutoBanking Customer Service workflow.\n\n"
        f"Conversation ID: {conversation_id}\n"
        f"Created At: {created_at}\n"
        f"Resolution Status: {resolved_status}\n"
        f"Summary: {summary}\n\n"
    )
    
    if recommendation:
        description += f"---- AI Recommendations ----\n{recommendation}\n\n"
    
    description += f"---- Conversation Transcript ----\n{preview}"
    
    return description
