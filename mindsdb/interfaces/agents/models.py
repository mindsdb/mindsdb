"""Pydantic models for agent interfaces.

These models provide structured data formats for agent inputs and outputs,
making it easier to validate, manipulate, and serialize data consistently.
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class AgentMessage(BaseModel):
    """A message sent to or received from an agent."""
    content: str
    role: Optional[str] = "user"


class AgentCompletion(BaseModel):
    """The completion result from an agent."""
    content: str
    context: List[Any] = Field(default_factory=list)
    trace_id: Optional[str] = None


class StreamChunk(BaseModel):
    """A chunk of streaming data from an agent."""
    type: str
    content: Optional[str] = None
    trace_id: str

    # For tool-related chunks
    tool: Optional[str] = None
    args: Optional[Dict[str, Any]] = None
    tool_call_id: Optional[str] = None

    # For model-related chunks
    index: Optional[int] = None
    args_delta: Optional[Dict[str, Any]] = None


class UserPromptChunk(StreamChunk):
    """A chunk representing a user prompt."""
    type: str = "user_prompt"
    content: str


class StartChunk(StreamChunk):
    """A chunk indicating the start of a stream."""
    type: str = "start"


class EndChunk(StreamChunk):
    """A chunk indicating the end of a stream."""
    type: str = "end"


class ErrorChunk(StreamChunk):
    """A chunk indicating an error."""
    type: str = "error"
    content: str


class ContentChunk(StreamChunk):
    """A chunk containing content text."""
    type: str = "chunk"
    content: str


class ToolCallChunk(StreamChunk):
    """A chunk representing a tool call."""
    type: str = "tool_call"
    tool: str
    args: Dict[str, Any]
    tool_call_id: str


class ToolResultChunk(StreamChunk):
    """A chunk representing a tool result."""
    type: str = "tool_result"
    content: str
    tool_call_id: str


class FinalChunk(StreamChunk):
    """A chunk representing the final result."""
    type: str = "final"
    content: str
