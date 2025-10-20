"""HTTP API routes and schemas for the AutoBankingCustomerService project."""

from __future__ import annotations

import time
from typing import List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .db import get_db_connection
from . import services

router = APIRouter()


class ConversationRequest(BaseModel):
    conversation_texts: List[str]


class SalesforceCase(BaseModel):
    conversation_id: str
    conversation_text: str
    summary: str | None
    status: str
    created_at: str
    processed_at: str
    jira_issue_key: str | None = None
    jira_issue_url: str | None = None
    jira_issue_error: str | None = None
    salesforce_case_id: str | None = None
    salesforce_case_url: str | None = None
    salesforce_error: str | None = None
    recommendation: str | None = None
    recommendation_error: str | None = None


class SalesforceResponse(BaseModel):
    success: bool
    total_conversations: int
    processed_count: int
    cases: List[SalesforceCase]
    processing_time_seconds: float


@router.get("/health")
def health_check():
    try:
        conn = get_db_connection(services.get_db_config())
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unhealthy: {exc}")

    conn.close()
    return {"status": "healthy", "database": "connected"}


@router.post("/api/process-conversations", response_model=SalesforceResponse)
async def process_conversations(request: ConversationRequest) -> SalesforceResponse:
    start_time = time.time()

    if not request.conversation_texts:
        raise HTTPException(status_code=400, detail="conversation_texts cannot be empty")

    cases_payload, processed_count = services.process_conversation_batch(
        request.conversation_texts
    )

    if not cases_payload:
        raise HTTPException(status_code=500, detail="No conversations could be processed")

    cases = [SalesforceCase(**payload) for payload in cases_payload]
    processing_time = round(time.time() - start_time, 2)

    return SalesforceResponse(
        success=True,
        total_conversations=len(request.conversation_texts),
        processed_count=processed_count,
        cases=cases,
        processing_time_seconds=processing_time,
    )
