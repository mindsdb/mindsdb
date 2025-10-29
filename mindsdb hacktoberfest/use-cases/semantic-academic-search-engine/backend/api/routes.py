"""API routes for MindsDB REST API."""
import logging
from typing import Any, Dict, List
from fastapi import APIRouter, HTTPException, status


from api.models import ChatCompletionRequest, ChatCompletionResponse, ChatInitiateResponse, ChatSessionRequest, HealthResponse, SearchRequest
from utils import build_create_agent_query, build_custom_prompt_template, build_psql_select_query, build_query_for_kb, generate_random_agent_name, replace_punctuation_with_underscore, transform_results
from config import get_config
from mindsdb import get_mindsdb_client

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    client = get_mindsdb_client()
    
    return HealthResponse(
        status="healthy" if client.is_connected() else "disconnected",
        connected=client.is_connected(),
        message="MindsDB client is ready" if client.is_connected() else "MindsDB client not connected"
    )

@router.post("/search", response_model=List[Dict[str, Any]])
async def search_knowledge_base(request: SearchRequest):
    """Search the knowledge base with the given query and filters.
    
    Args:
        request: Search request containing query and optional filters
        
    Returns:
        Search results from the knowledge base
        
    Example:
        ```
        POST /search
        {
          "query": "machine learning",
          "filters": {
            "isHybridSearch": true,
            "alpha": 0.7,
            "corpus": {
              "arxiv": true,
              "patent": false
            },
            "publishedYear": "2024",
            "category": "cs.LG"
          }
        }
        ```
    """
    client = get_mindsdb_client()
    config = get_config()
    
    if not client.is_connected():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Not connected to MindsDB"
        )
    
    # Get knowledge base name from configuration
    kb_name = config.get("knowledge_base.name", "my_pg_kb")
    
    query_clause = f"content = '{request.query}'";
    clauses = [query_clause]
    if request.filters:
        filters = request.filters

        if filters.isHybridSearch:
            clauses.append(f"hybrid_search = true AND hybrid_search_alpha = {filters.alpha}")
    
        corpus_filters = filters.corpus.model_dump()
        if not all(corpus_filters.values()):
            source_clause = [
                f"source = '{source}'" 
                for source, enabled in corpus_filters.items() 
                if enabled
            ]
            if source_clause:
                clauses.append(f"({' OR '.join(source_clause)})")
        
        if filters.publishedYear:
            clauses.append(f"published_year = '{filters.publishedYear}'")
        
        if filters.category:
            clauses.append(f"categories LIKE '%{filters.category.lower()}%'")
    
    search_query = f"""
SELECT article_id, metadata, relevance FROM {kb_name}
WHERE {" AND ".join(clauses)};
"""
    logger.info(f"search query - {search_query}")

    try:
        # Execute query
        mdb_query = client.query(search_query)
        
        # Convert result to list of dictionaries
        result = mdb_query.fetch()
        data = result.to_dict("records")
        return transform_results(data)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}"
        )

@router.post("/chat/initiate", response_model=ChatInitiateResponse)
async def initiate_chat(request: ChatSessionRequest):

    client = get_mindsdb_client()
    
    if not client.is_connected():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Not connected to MindsDB"
        )

    res = []
    kb_list = []
    prompt_template_list = []
    for paper in request.papers:

        kb_name = f"{replace_punctuation_with_underscore(paper.id)}_kb".lower()
        create_kb_query = build_query_for_kb(kb_name, 'create')
        
        result = client.query(create_kb_query)
        _ = result.fetch()

        insert_kb_query = build_query_for_kb(kb_name, 'insert')
        add_clause = f"WHERE article_id = '{paper.id}' AND source = '{paper.source}'";
        insert_kb_query = f"{insert_kb_query} {add_clause}"
        
        result = client.query(insert_kb_query)
        _ = result.fetch()

        create_kb_index_query = f"CREATE INDEX ON KNOWLEDGE_BASE {kb_name};"
        result = client.query(create_kb_index_query)
        _ = result.fetch()

        psql_query = build_psql_select_query()
        psql_query = f"{psql_query} {add_clause}"
        
        result = client.query(psql_query)
        data = result.fetch()

        data = data.to_dict("records")

        paper_res = {}
        paper_res["paperUrl"] = data[0].get("pdf_url")
        paper_res["title"] = data[0].get("title")
        paper_res["source"] = data[0].get("source")
        paper_res["paperId"] = paper.id

        res.append(paper_res)
        kb_list.append(kb_name)
        prompt_template_list.append(f"mindsdb.{kb_name} stores the data about the {'patent' if paper_res['source'] == 'patent' else 'research paper'} with title {paper_res['title']}")

    # create agent
    agent_name = generate_random_agent_name()
    prompt_template_kbs = '\n'.join(prompt_template_list)
    prompt_template = build_custom_prompt_template(prompt_template_kbs)
    create_agent_query = build_create_agent_query(agent_name, kb_list, [], prompt_template)
    result = client.query(create_agent_query)
    _ = result.fetch()

    temp = {'aiAgentId': agent_name, 'documents': res}
    return ChatInitiateResponse(**temp)

@router.post("/chat/completion", response_model=ChatCompletionResponse)
async def chat_with_papers(request: ChatCompletionRequest):

    client = get_mindsdb_client()
    agent_name = request.agentId
    chat_query = f'SELECT answer FROM {agent_name} WHERE question = "{request.query}";'

    mdb_query = client.query(chat_query)
    result = mdb_query.fetch()
    result = result.to_dict("records")
    if not result or len(result) == 0:
        logger.warning(f"No response received from agent '{agent_name}'")
        return ""

    # Extract answer from the first result
    first_result = result[0]
    if not isinstance(first_result, dict) or "answer" not in first_result:
        logger.warning(f"Unexpected response format from agent '{agent_name}'")
        return ""

    answer = first_result["answer"]
    logger.debug(
        f"Received response from agent '{agent_name}': {len(str(answer))} characters"
    )

    return ChatCompletionResponse(answer=answer)


