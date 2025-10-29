import json
import logging
import random
import string
from typing import Any, Dict, List

from config.loader import get_config


logger = logging.getLogger(__name__)

def transform_results(results_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform and clean search results.

    Args:
        results_list: Raw search results

    Returns:
        List of cleaned and deduplicated results
    """
    logger.info(f"Transforming {len(results_list)} search results")

    cleaned_results = []
    seen_articles = set()
    skipped_count = 0

    for i, result in enumerate(results_list):
        try:
            metadata = json.loads(result["metadata"])
            article_id = metadata["article_id"]

            # Skip duplicate articles
            if article_id in seen_articles:
                skipped_count += 1
                logger.debug(f"Skipping duplicate article: {article_id}")
                continue

            cleaned_result = {
                "id": article_id,
                "authors": metadata["authors"],
                "categories": [metadata["categories"]],
                "source": metadata["source"],
                "published_year": metadata["published_year"],
                "title": metadata.get("title", ""),
                "abstract": metadata.get("abstract", ""),
                "relevance": round(result["relevance"], 3) if result["relevance"] else result["relevance"],
            }

            seen_articles.add(article_id)
            cleaned_results.append(cleaned_result)
            logger.debug(f"Processed result {i + 1}/{len(results_list)}: {article_id}")

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Error processing result {i + 1}/{len(results_list)}: {e}")
            skipped_count += 1
            continue

    logger.info(
        f"Transformation complete: {len(cleaned_results)} valid results, {skipped_count} skipped"
    )
    return cleaned_results

def build_query_for_kb(kb_name: str, type: str):
    config = get_config()
    
    # Load knowledge base configuration
    db_name = config.get("pgvector.database_name", "my_pgvector")
    storage_table = config.get("knowledge_base.storage_table", "pgvector_storage_table")
    
    # Embedding model config
    emb_model = config.get("knowledge_base.embedding_model", {})
    emb_provider = emb_model.get("provider", "openai")
    emb_model_name = emb_model.get("model_name", "text-embedding-3-small")
    emb_api_key = emb_model.get("api_key", "")
    
    # Reranking model config
    rerank_model = config.get("knowledge_base.reranking_model", {})
    rerank_provider = rerank_model.get("provider", "openai")
    rerank_model_name = rerank_model.get("model_name", "gpt-4o")
    rerank_api_key = rerank_model.get("api_key", "")
    
    # Metadata and content columns
    metadata_cols = config.get("knowledge_base.metadata_columns", ["product"])
    content_cols = config.get("knowledge_base.content_columns", ["notes"])

    logger.info(f"Setting up knowledge base: {kb_name}")
    logger.info(f"  Storage: {db_name}.{storage_table}")
    logger.info(f"  Embedding model: {emb_provider}/{emb_model_name}")
    
    # Check if API key is set
    if not emb_api_key or (emb_api_key.startswith("sk-proj-") and len(emb_api_key) < 20):
        logger.warning("OpenAI API key not set, skipping knowledge base creation")
        logger.warning("Set OPENAI_API_KEY in .env file to enable knowledge base")
        return {
            "success": True,
            "operation": "knowledge_base",
            "message": "Skipped (OpenAI API key not configured)",
            "skipped": True
        }
    
    # Build CREATE KNOWLEDGE_BASE query
    metadata_str = "[" + ", ".join([f"'{col}'" for col in metadata_cols]) + "]"
    content_str = "[" + ", ".join([f"'{col}'" for col in content_cols]) + "]"

    if type == 'create':
        create_kb_query = f"""
    CREATE KNOWLEDGE_BASE IF NOT EXISTS {kb_name}
    USING
    storage = {db_name}.{storage_table},
    embedding_model = {{
        "provider": "{emb_provider}",
        "model_name": "{emb_model_name}",
        "api_key": "{emb_api_key}"
    }},
    reranking_model = {{
        "provider": "{rerank_provider}",
        "model_name": "{rerank_model_name}",
        "api_key": "{rerank_api_key}"
    }},
    metadata_columns = {metadata_str},
    content_columns = {content_str};
    """
        return create_kb_query
    
    
    psql_query = build_psql_select_query()

    insert_kb_query = f"""
    INSERT INTO {kb_name}
        {psql_query}
    """

    return insert_kb_query

def build_psql_select_query():
    config = get_config()

    db_name = config.get("pgvector.database_name", "my_pgvector")
    metadata_cols = config.get("knowledge_base.metadata_columns", ["product"])
    content_cols = config.get("knowledge_base.content_columns", ["notes"])
    all_columns = content_cols + metadata_cols
    columns_str = ", ".join(all_columns)

    select_psql_query = f"""
    SELECT {columns_str}
        FROM {db_name}.paper_raw
    """
    return select_psql_query

def build_create_agent_query(name, kb_list, table_list, prompt_template):
    config = get_config()

    agent_model = config.get("agent.model", {})
    api_key = config.get("knowledge_base.embedding_model.api_key", {})
    query = f"""
    CREATE AGENT IF NOT EXISTS {name}
    USING
        model = {{
            "provider": "openai",
            "model_name" : '{agent_model}',
            "api_key": '{api_key}'
        }},
        data = {{
            "knowledge_bases": {kb_list},
            "tables": {table_list}    
        }},
        prompt_template = "{prompt_template}";
    """

    return query

def generate_random_agent_name():
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(4)).lower()

def replace_punctuation_with_underscore(text):
    translator = str.maketrans(string.punctuation, '_' * len(string.punctuation))
    return text.translate(translator)

def build_custom_prompt_template(kbs):
    return  f"""You are an expert research assistant with access to multiple scientific papers and patents. Your knowledge base contains documents from various sources including arXiv, medRxiv, chemRxiv, bioRxiv, and patent databases.

CORE RESPONSIBILITIES:
1. Answer questions using ONLY information from the papers in your knowledge base
2. Synthesize information across multiple papers when relevant
3. Always cite specific papers when providing information
4. Clearly state when information is not available in your knowledge base
5. Compare and contrast findings from different papers when asked

RESPONSE GUIDELINES:
- Be precise and use scientific language appropriate for the domain
- When citing, mention the paper title or identifier
- If multiple papers address the same topic, acknowledge different perspectives or findings
- For questions outside your knowledge base, clearly state: I don't have information about this in the available papers
- Explain technical terminology when it helps understanding
- If papers conflict, present both viewpoints objectively

KNOWLEDGE BASE:
You have access to the following papers covering various research domains. Use the context from all relevant papers to provide comprehensive answers.

{kbs}

Provide a clear, well-structured answer based on the available research papers in markdown format. """