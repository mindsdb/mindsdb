"""Business logic for processing banking customer conversations."""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import HTTPException

from . import db

classification_agent = None
_mindsdb_server = None
_db_config_override: Optional[Dict[str, Any]] = None


def set_agent(agent: Any) -> None:
    global classification_agent
    classification_agent = agent


def set_mindsdb_server(server: Any) -> None:
    global _mindsdb_server
    _mindsdb_server = server


def set_db_config(config: Dict[str, Any]) -> None:
    global _db_config_override
    _db_config_override = config


def get_db_config() -> Optional[Dict[str, Any]]:
    return _db_config_override


def clear_state() -> None:
    global classification_agent, _mindsdb_server
    classification_agent = None
    _mindsdb_server = None


def query_agent(conversation_text: str) -> Dict[str, Any]:
    if classification_agent is None:
        raise HTTPException(
            status_code=503,
            detail="Classification agent not initialized. Make sure MindsDB is running.",
        )

    try:
        completion = classification_agent.completion(
            [
                {
                    "question": conversation_text,
                    "answer": None,
                }
            ]
        )
    except Exception as exc:  # pragma: no cover - network failure guard
        raise HTTPException(status_code=500, detail=f"Agent query failed: {exc}") from exc

    response_text = completion.content

    summary_match = re.search(r"Summary:\s*(.+?)(?=Status:)", response_text, re.DOTALL)
    status_match = re.search(
        r"Status:\s*(RESOLVED|UNRESOLVED)", response_text, re.IGNORECASE
    )

    summary = summary_match.group(1).strip() if summary_match else response_text.strip()
    status = status_match.group(1).upper() if status_match else "UNRESOLVED"

    return {"summary": summary, "resolved": status == "RESOLVED"}


def process_conversation(conversation_text: str) -> Dict[str, Any]:
    analysis = query_agent(conversation_text)
    conversation_id = str(uuid.uuid4())

    db.insert_conversation_with_analysis(
        conversation_id=conversation_id,
        conversation_text=conversation_text,
        summary=analysis["summary"],
        resolved=analysis["resolved"],
        db_config=_db_config_override,
    )

    timestamp = datetime.now().isoformat()
    display_text = (
        conversation_text[:500] + "..." if len(conversation_text) > 500 else conversation_text
    )

    return {
        "conversation_id": conversation_id,
        "conversation_text": display_text,
        "summary": analysis["summary"],
        "status": "RESOLVED" if analysis["resolved"] else "UNRESOLVED",
        "created_at": timestamp,
        "processed_at": timestamp,
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
