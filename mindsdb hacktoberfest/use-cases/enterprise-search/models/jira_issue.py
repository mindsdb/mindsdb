from typing import Optional

from pydantic import BaseModel


class JiraIssue(BaseModel):
    id: str
    key: str
    project_id: Optional[str] = None
    project_key: Optional[str] = None
    project_name: Optional[str] = None
    summary: Optional[str] = None
    priority: Optional[str] = None
    creator: Optional[str] = None
    assignee: Optional[str] = None
    status: Optional[str] = None
    description: Optional[str] = None

    model_config = {"extra": "ignore"}
