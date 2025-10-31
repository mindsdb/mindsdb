"""Pydantic models for Confluence data."""

from typing import Optional

from pydantic import BaseModel


class ConfluencePage(BaseModel):
    """Model for Confluence pages."""

    id: str
    status: str
    title: str
    spaceId: str
    parentId: Optional[str] = None
    parentType: Optional[str] = None
    position: Optional[int] = None
    authorId: Optional[str] = None
    ownerId: Optional[str] = None
    lastOwnerId: Optional[str] = None
    createdAt: Optional[str] = None
    version_createdAt: Optional[str] = None
    version_message: Optional[str] = None
    version_number: Optional[int] = None
    version_minorEdit: Optional[bool] = None
    version_authorId: Optional[str] = None
    body_storage_representation: Optional[str] = None
    body_storage_value: Optional[str] = None
    _links_webui: Optional[str] = None
    _links_editui: Optional[str] = None
    _links_tinyui: Optional[str] = None

    model_config = {"extra": "ignore"}
