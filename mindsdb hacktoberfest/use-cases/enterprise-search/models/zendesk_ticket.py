from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel


@dataclass
class ZendeskTicket(BaseModel):
    id: str
    subject: Optional[str] = None
    description: Optional[str] = None
    priority: Optional[str] = None
    status: Optional[str] = None
    assignee_id: Optional[str] = None
    requester_id: Optional[str] = None
    brand_id: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    type: Optional[str] = None
    tags: Optional[str] = None
    url: Optional[str] = None

    # # Additional fields from Zendesk API
    # collaborator_ids: Optional[List[str]] = None
    # custom_fields: Optional[List[Dict[str, Any]]] = None
    # due_at: Optional[str] = None
    # external_id: Optional[str] = None
    # fields: Optional[List[Dict[str, Any]]] = None
    # forum_topic_id: Optional[str] = None
    # group_id: Optional[str] = None
    # has_incidents: Optional[bool] = None
    # organization_id: Optional[str] = None
    # problem_id: Optional[str] = None
    # raw_subject: Optional[str] = None
    # recipient: Optional[str] = None
    # sharing_agreement_ids: Optional[List[str]] = None
    # submitter_id: Optional[str] = None
    # generated_timestamp: Optional[int] = None
    # follower_ids: Optional[List[str]] = None
    # email_cc_ids: Optional[List[str]] = None
    # is_public: Optional[bool] = None
    # custom_status_id: Optional[str] = None
    # followup_ids: Optional[List[str]] = None
    # ticket_form_id: Optional[str] = None
    # allow_channelback: Optional[bool] = None
    # allow_attachments: Optional[bool] = None
    # from_messaging_channel: Optional[bool] = None

    # # Satisfaction rating object
    # satisfaction_rating: Optional[Dict[str, Any]] = None
    model_config = {"extra": "ignore"}
