"""@private"""

from datetime import datetime
from langfuse.client import PromptClient, ModelUsage, MapValue
from typing import Any, List, Optional, TypedDict, Literal, Dict, Union
from pydantic import BaseModel

SpanLevel = Literal["DEBUG", "DEFAULT", "WARNING", "ERROR"]


class TraceMetadata(TypedDict):
    name: Optional[str]
    user_id: Optional[str]
    session_id: Optional[str]
    version: Optional[str]
    release: Optional[str]
    metadata: Optional[Any]
    tags: Optional[List[str]]
    public: Optional[bool]


class ObservationParams(TraceMetadata, TypedDict):
    input: Optional[Any]
    output: Optional[Any]
    level: Optional[SpanLevel]
    status_message: Optional[str]
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    completion_start_time: Optional[datetime]
    model: Optional[str]
    model_parameters: Optional[Dict[str, MapValue]]
    usage: Optional[Union[BaseModel, ModelUsage]]
    prompt: Optional[PromptClient]
