from typing import List, Optional
from pydantic import BaseModel, Field, HttpUrl

class CorpusFilter(BaseModel):
    """Corpus filter for search."""
    arxiv: bool = Field(default=True, description="Include arXiv papers")
    patent: bool = Field(default=True, description="Include patents")
    biorxiv: bool = Field(default=True, description="Include biorxiv papers")
    medrxiv: bool = Field(default=True, description="Include medrxiv papers")
    chemrxiv: bool = Field(default=True, description="Include chemrxiv papers")

class SearchFilters(BaseModel):
    """Search filters."""
    isHybridSearch: Optional[bool] = Field(default=True, description="Enable hybrid search")
    alpha: Optional[float] = Field(default=0.7, ge=0.0, le=1.0, description="Hybrid search alpha parameter")
    corpus: Optional[CorpusFilter] = Field(default=None, description="Corpus filters")
    publishedYear: Optional[str] = Field(default=None, description="Filter by published year")
    category: Optional[str] = Field(default=None, description="Filter by category")

class SearchRequest(BaseModel):
    """Knowledge base search request."""
    query: str = Field(..., description="Search query text")
    filters: Optional[SearchFilters] = Field(default=None, description="Search filters")

class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    connected: bool
    message: str

class Paper(BaseModel):
    id: str = Field(..., description="Paper id")
    source: str = Field(..., description="Paper source")

class ChatSessionRequest(BaseModel):
    """Create chat search request."""
    papers: List[Paper] = Field(..., description="List of papers")

class ChatCompletionRequest(BaseModel):
    """AI agent chat request."""
    query: str = Field(..., description="chat query text")
    agentId: str = Field(..., description="agent id")

class Document(BaseModel):
    paperUrl: HttpUrl = Field(..., description="URL of the paper PDF")
    title: str = Field(..., description="Title of the paper")
    source: str = Field(..., description="Source of the document")
    paperId: str = Field(..., description="Unique paper identifier")

class ChatInitiateResponse(BaseModel):
    aiAgentId: str = Field(..., description="AI Agent identifier")
    documents: List[Document] = Field(..., description="List of documents")

class ChatCompletionResponse(BaseModel):
    """AI agent response."""
    answer: str = Field(..., description="chat query text")
