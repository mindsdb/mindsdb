from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import date

from src.knowledge_base.queries import (
    query_academic_papers,
    semantic_search,
    hybrid_search
)

# Create routers
health_router = APIRouter()
search_router = APIRouter()


# Request/Response Models
class SearchRequest(BaseModel):
    """Request model for search operations"""
    query: str = Field(..., min_length=1, max_length=500, description="Search query")
    limit: int = Field(default=10, ge=1, le=100, description="Maximum number of results")
    

class SemanticSearchRequest(BaseModel):
    """Request model for semantic search"""
    query: str = Field(..., min_length=1, max_length=500, description="Search query")
    threshold: float = Field(default=0.7, ge=0.0, le=1.0, description="Similarity threshold")


class HybridSearchRequest(BaseModel):
    """Request model for hybrid search"""
    query: str = Field(..., min_length=1, max_length=500, description="Search query")
    metadata_filters: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="Metadata filters (e.g., {'authors': 'John Doe', 'year': 2023})"
    )
    limit: int = Field(default=10, ge=1, le=100, description="Maximum number of results")


class PaperResponse(BaseModel):
    """Response model for a single paper"""
    entry_id: str
    title: str
    summary: str
    authors: str
    published_date: Optional[date] = None
    pdf_url: Optional[str] = None
    relevance_score: Optional[float] = None


class SearchResponse(BaseModel):
    """Response model for search results"""
    query: str
    results: List[PaperResponse]
    total_results: int
    

class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str
    message: str


# Health Check Endpoint
@health_router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint to verify API is running
    """
    return HealthResponse(
        status="healthy",
        message="Academic Research Copilot API is running"
    )


# Search Endpoints
@search_router.post("/search", response_model=SearchResponse)
async def search_papers(request: SearchRequest):
    """
    Search academic papers using basic query
    
    - **query**: The search query string
    - **limit**: Maximum number of results to return (1-100)
    """
    try:
        results = await query_academic_papers(request.query, request.limit)
        
        papers = [
            PaperResponse(
                entry_id=paper.get("entry_id", ""),
                title=paper.get("title", ""),
                summary=paper.get("summary", ""),
                authors=paper.get("authors", ""),
                published_date=paper.get("published_date"),
                pdf_url=paper.get("pdf_url"),
                relevance_score=paper.get("relevance_score")
            )
            for paper in results
        ]
        
        return SearchResponse(
            query=request.query,
            results=papers,
            total_results=len(papers)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@search_router.post("/search/semantic", response_model=SearchResponse)
async def semantic_search_papers(request: SemanticSearchRequest):
    """
    Perform semantic search on academic papers
    
    - **query**: The search query string
    - **threshold**: Minimum similarity score (0.0-1.0)
    """
    try:
        results = await semantic_search(request.query, request.threshold)
        
        papers = [
            PaperResponse(
                entry_id=paper.get("entry_id", ""),
                title=paper.get("title", ""),
                summary=paper.get("summary", ""),
                authors=paper.get("authors", ""),
                published_date=paper.get("published_date"),
                pdf_url=paper.get("pdf_url"),
                relevance_score=paper.get("relevance_score")
            )
            for paper in results
        ]
        
        return SearchResponse(
            query=request.query,
            results=papers,
            total_results=len(papers)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Semantic search failed: {str(e)}")


@search_router.post("/search/hybrid", response_model=SearchResponse)
async def hybrid_search_papers(request: HybridSearchRequest):
    """
    Perform hybrid search combining semantic and metadata filtering
    
    - **query**: The search query string
    - **metadata_filters**: Dictionary of metadata filters to apply
    - **limit**: Maximum number of results to return (1-100)
    """
    try:
        results = await hybrid_search(request.query, request.metadata_filters)
        
        # Apply limit
        results = results[:request.limit]
        
        papers = [
            PaperResponse(
                entry_id=paper.get("entry_id", ""),
                title=paper.get("title", ""),
                summary=paper.get("summary", ""),
                authors=paper.get("authors", ""),
                published_date=paper.get("published_date"),
                pdf_url=paper.get("pdf_url"),
                relevance_score=paper.get("relevance_score")
            )
            for paper in results
        ]
        
        return SearchResponse(
            query=request.query,
            results=papers,
            total_results=len(papers)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Hybrid search failed: {str(e)}")


@search_router.get("/papers/{entry_id}", response_model=PaperResponse)
async def get_paper_by_id(entry_id: str):
    """
    Get a specific paper by its entry ID
    
    - **entry_id**: The unique identifier of the paper
    """
    try:
        # This would need to be implemented in queries.py
        from src.knowledge_base.queries import get_paper_by_id
        paper = await get_paper_by_id(entry_id)
        
        if not paper:
            raise HTTPException(status_code=404, detail="Paper not found")
        
        return PaperResponse(
            entry_id=paper.get("entry_id", ""),
            title=paper.get("title", ""),
            summary=paper.get("summary", ""),
            authors=paper.get("authors", ""),
            published_date=paper.get("published_date"),
            pdf_url=paper.get("pdf_url")
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve paper: {str(e)}")
