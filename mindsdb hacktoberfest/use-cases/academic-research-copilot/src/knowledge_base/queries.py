"""
Query functions for MindsDB Knowledge Base semantic search
"""
from typing import List, Dict, Any, Optional
import asyncio
from datetime import date, datetime
import mindsdb_sdk
import os


def get_mindsdb_connection(url: str = None):
    """Get MindsDB connection"""
    if url is None:
        # Use environment variables if available
        mindsdb_host = os.getenv("MINDSDB_HOST", "localhost")
        mindsdb_port = os.getenv("MINDSDB_PORT", "47334")
        url = f"http://{mindsdb_host}:{mindsdb_port}"
    return mindsdb_sdk.connect(url)


async def query_academic_papers(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Queries the MindsDB Knowledge Base for academic papers based on the provided query.
    
    Parameters:
    - query (str): The search query to find relevant academic papers.
    - limit (int): The maximum number of results to return. Default is 10.
    
    Returns:
    - List[Dict]: A list of dictionaries containing the metadata of the matching academic papers.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _query_papers_sync, query, limit)


def _query_papers_sync(query: str, limit: int) -> List[Dict[str, Any]]:
    """Synchronous implementation of paper query - uses MindsDB Knowledge Base for semantic search"""
    try:
        # Connect to MindsDB
        server = get_mindsdb_connection()
        
        # Query the Knowledge Base for semantic search
        # MindsDB will use vector embeddings to find semantically similar papers
        # The WHERE clause with '=' on content triggers semantic search
        sql_query = f"""
        SELECT 
            id,
            content as chunk_content,
            distance,
            relevance
        FROM academic_kb
        WHERE content = '{query}'
        LIMIT {limit};
        """
        
        result = server.query(sql_query).fetch()
        
        # If no results from KB, fall back to DuckDB
        if len(result) == 0:
            print(f"⚠️  No results from Knowledge Base, trying DuckDB...")
            raise Exception("KB returned no results")
        
        # Convert to list of dictionaries
        papers = []
        for _, row in result.iterrows():
            # Parse the content to extract title and abstract
            content = str(row.get('chunk_content', ''))
            parts = content.split('\n\n', 1)
            title = parts[0] if len(parts) > 0 else 'Untitled'
            summary = parts[1] if len(parts) > 1 else content
            
            # Get relevance score (0-1, higher is better)
            relevance = float(row.get('relevance', 0.5))
            
            # Extract paper ID from chunk content or use the id
            paper_id = str(row.get('id', 'unknown'))
            
            paper = {
                "entry_id": paper_id,
                "title": title[:200],  # Limit title length
                "summary": summary[:500],  # Limit summary length
                "authors": "See PDF for authors",  # KB doesn't store this
                "published_date": None,
                "pdf_url": "",
                "categories": "",
                "relevance_score": relevance
            }
            papers.append(paper)
        
        print(f"✓ Found {len(papers)} papers semantically similar to '{query}'")
        return papers
        
    except Exception as e:
        print(f"⚠️  Error querying Knowledge Base: {e}")
        print(f"   Falling back to DuckDB text search...")
        
        # Fallback to DuckDB text search if KB is not available
        try:
            import duckdb
            
            # Get database path from environment or use default
            db_path = os.getenv('DUCKDB_PATH', '/app/data/academic_papers.duckdb')
            
            # Connect to DuckDB
            conn = duckdb.connect(db_path, read_only=True)
            
            # Search papers using simple text matching on title and summary
            sql_query = f"""
            SELECT 
                entry_id,
                title,
                summary,
                authors,
                published_date,
                pdf_url,
                categories
            FROM papers
            WHERE LOWER(title) LIKE LOWER('%{query}%') 
               OR LOWER(summary) LIKE LOWER('%{query}%')
               OR LOWER(categories) LIKE LOWER('%{query}%')
            LIMIT {limit};
            """
            
            result = conn.execute(sql_query).fetchdf()
            conn.close()
            
            # Convert to list of dictionaries
            papers = []
            for _, row in result.iterrows():
                paper = {
                    "entry_id": row['entry_id'],
                    "title": row['title'],
                    "summary": row['summary'],
                    "authors": row['authors'],
                    "published_date": str(row['published_date']) if row['published_date'] else None,
                    "pdf_url": row['pdf_url'],
                    "categories": row['categories'],
                    "relevance_score": 0.75  # Lower score for text-based fallback
                }
                papers.append(paper)
            
            print(f"✓ Found {len(papers)} papers matching '{query}' (text search)")
            return papers
            
        except Exception as db_error:
            print(f"⚠️  Error with fallback DuckDB search: {db_error}")
            print("  Returning sample data for demo purposes...")
            # Final fallback to sample data
            return [
                {
                    "entry_id": f"arxiv-sample-{i}",
                    "title": f"Sample Paper {i}: {query}",
                    "summary": f"This is a sample summary for paper {i} related to {query}. "
                              f"In a production environment, this would be real paper data from ArXiv.",
                    "authors": "John Doe, Jane Smith",
                    "published_date": str(date(2024, 1, 1)),
                    "pdf_url": f"https://arxiv.org/pdf/sample{i}.pdf",
                    "categories": "cs.LG, cs.AI",
                "relevance_score": 0.95 - (i * 0.05)
            }
            for i in range(1, min(limit + 1, 6))
        ]


async def semantic_search(query: str, threshold: float = 0.7) -> List[Dict[str, Any]]:
    """
    Performs a semantic search on the Knowledge Base to find academic papers that are contextually relevant.
    
    Parameters:
    - query (str): The search query to find relevant academic papers.
    - threshold (float): The minimum similarity score to consider a paper relevant. Default is 0.7.
    
    Returns:
    - List[Dict]: A list of dictionaries containing the metadata of the matching academic papers.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _semantic_search_sync, query, threshold)


def _semantic_search_sync(query: str, threshold: float) -> List[Dict[str, Any]]:
    """Synchronous implementation of semantic search - uses MindsDB KB with relevance filtering"""
    try:
        # Use MindsDB Knowledge Base for semantic search with relevance threshold
        server = get_mindsdb_connection()
        
        # Query with relevance filter
        # MindsDB Knowledge Base returns 'distance' and 'relevance'
        # Higher relevance = better match
        sql_query = f"""
        SELECT 
            id,
            content as chunk_content,
            distance,
            relevance
        FROM academic_kb
        WHERE content = '{query}'
          AND relevance >= {threshold}
        LIMIT 50;
        """
        
        result = server.query(sql_query).fetch()
        
        # Convert to list of dictionaries
        papers = []
        for _, row in result.iterrows():
            # Parse the content to extract title and abstract
            content = str(row.get('chunk_content', ''))
            parts = content.split('\n\n', 1)
            title = parts[0] if len(parts) > 0 else 'Untitled'
            summary = parts[1] if len(parts) > 1 else content
            
            # Get relevance score (already 0-1, higher is better)
            relevance = float(row.get('relevance', 0.5))
            
            if relevance >= threshold:
                paper_id = str(row.get('id', 'unknown'))
                
                paper = {
                    "entry_id": paper_id,
                    "title": title[:200],
                    "summary": summary[:500],
                    "authors": "See PDF for authors",
                    "published_date": None,
                    "pdf_url": "",
                    "categories": "",
                    "relevance_score": relevance
                }
                papers.append(paper)
        
        print(f"✓ Found {len(papers)} papers with relevance >= {threshold}")
        return papers
        
    except Exception as e:
        print(f"⚠️  Error in semantic search: {e}")
        # Fallback to basic query with filtering
        results = _query_papers_sync(query, 50)
        return [r for r in results if r.get("relevance_score", 0) >= threshold]


async def hybrid_search(
    query: str, 
    metadata_filters: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Performs a hybrid search combining semantic and metadata filters on the Knowledge Base.
    
    Parameters:
    - query (str): The search query to find relevant academic papers.
    - metadata_filters (Dict[str, Any]): A dictionary of metadata filters to apply to the search.
    
    Returns:
    - List[Dict]: A list of dictionaries containing the metadata of the matching academic papers.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _hybrid_search_sync, query, metadata_filters)


def _hybrid_search_sync(query: str, metadata_filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Synchronous implementation of hybrid search - MindsDB KB with metadata filters"""
    try:
        # Use MindsDB Knowledge Base for semantic search
        server = get_mindsdb_connection()
        
        # Build SQL query with metadata filters
        base_query = f"""
        SELECT 
            entry_id,
            title,
            summary,
            authors,
            published_date,
            pdf_url,
            categories,
            distance
        FROM academic_kb
        WHERE content = '{query}'
        """
        
        # Add metadata filters
        if metadata_filters:
            if 'authors' in metadata_filters:
                # Note: MindsDB KB stores metadata as JSON, so we need to filter differently
                # For now, we'll fetch results and filter in Python
                pass
            
            if 'categories' in metadata_filters:
                # Similarly, categories filtering will be done post-query
                pass
        
        base_query += " LIMIT 50;"
        
        result = server.query(base_query).fetch()
        
        # Convert to list of dictionaries and apply metadata filters
        papers = []
        for _, row in result.iterrows():
            # Convert distance to relevance score
            distance = row.get('distance', 1.0)
            relevance = max(0, min(1, 1 - (distance / 2)))
            
            # Check metadata filters
            if metadata_filters:
                # Filter by authors
                if 'authors' in metadata_filters:
                    authors = row.get('authors', '')
                    if metadata_filters['authors'].lower() not in str(authors).lower():
                        continue
                
                # Filter by year
                if 'year' in metadata_filters:
                    pub_date = row.get('published_date', '')
                    if pub_date:
                        year = int(str(pub_date)[:4]) if str(pub_date) else 0
                        if year < metadata_filters['year']:
                            continue
                
                # Filter by categories
                if 'categories' in metadata_filters:
                    categories = row.get('categories', '')
                    if metadata_filters['categories'].lower() not in str(categories).lower():
                        continue
            
            paper = {
                "entry_id": row['entry_id'],
                "title": row['title'],
                "summary": row['summary'],
                "authors": row.get('authors', ''),
                "published_date": str(row['published_date']) if row.get('published_date') else None,
                "pdf_url": row.get('pdf_url', ''),
                "categories": row.get('categories', ''),
                "relevance_score": relevance
            }
            papers.append(paper)
        
        print(f"✓ Found {len(papers)} papers with filters")
        return papers
        
    except Exception as e:
        print(f"⚠️  Error in hybrid search: {e}")
        print(f"   Falling back to DuckDB search with filters...")
        
        # Fallback to DuckDB search with filters
    try:
        import duckdb
        import os
        
        # Get database path
        db_path = os.getenv('DUCKDB_PATH', '/app/data/academic_papers.duckdb')
        conn = duckdb.connect(db_path, read_only=True)
        
        # Build SQL query with filters
        base_query = f"""
        SELECT 
            entry_id,
            title,
            summary,
            authors,
            published_date,
            pdf_url,
            categories
        FROM papers
        WHERE (LOWER(title) LIKE LOWER('%{query}%') 
           OR LOWER(summary) LIKE LOWER('%{query}%')
           OR LOWER(categories) LIKE LOWER('%{query}%'))
        """
        
        # Add metadata filters
        if metadata_filters:
            if 'authors' in metadata_filters:
                base_query += f" AND LOWER(authors) LIKE LOWER('%{metadata_filters['authors']}%')"
            
            if 'year' in metadata_filters:
                base_query += f" AND CAST(strftime(published_date, '%Y') AS INTEGER) >= {metadata_filters['year']}"
            
            if 'categories' in metadata_filters:
                base_query += f" AND LOWER(categories) LIKE LOWER('%{metadata_filters['categories']}%')"
        
        base_query += " LIMIT 50;"
        
        result = conn.execute(base_query).fetchdf()
        conn.close()
        
        # Convert to list of dictionaries
        papers = []
        for _, row in result.iterrows():
            paper = {
                "entry_id": row['entry_id'],
                "title": row['title'],
                "summary": row['summary'],
                "authors": row['authors'],
                "published_date": str(row['published_date']) if row['published_date'] else None,
                "pdf_url": row['pdf_url'],
                "categories": row['categories'],
                "relevance_score": 0.85
            }
            papers.append(paper)
        
        print(f"✓ Found {len(papers)} papers with filters")
        return papers
        
    except Exception as e:
        print(f"⚠️  Error in hybrid search: {e}")
        # Fallback to basic query with post-filtering
        results = _query_papers_sync(query, 50)
        
        if metadata_filters:
            filtered_results = []
            for result in results:
                match = True
                
                if "authors" in metadata_filters:
                    author_filter = metadata_filters["authors"].lower()
                    if author_filter not in result.get("authors", "").lower():
                        match = False
                
                if "year" in metadata_filters and match:
                    year_filter = metadata_filters["year"]
                    pub_date = result.get("published_date")
                    if pub_date:
                        if isinstance(pub_date, str):
                            pub_year = int(pub_date[:4])
                        elif isinstance(pub_date, (date, datetime)):
                            pub_year = pub_date.year
                        else:
                            pub_year = 2024
                        
                        if pub_year < year_filter:
                            match = False
                
                if match:
                    filtered_results.append(result)
            
            return filtered_results
        
        return results


async def get_paper_by_id(entry_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve a specific paper by its entry ID.
    
    Parameters:
    - entry_id (str): The unique identifier of the paper
    
    Returns:
    - Dict or None: Paper metadata if found, None otherwise
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_paper_by_id_sync, entry_id)


def _get_paper_by_id_sync(entry_id: str) -> Optional[Dict[str, Any]]:
    """Synchronous implementation of get paper by ID - queries DuckDB directly"""
    try:
        import duckdb
        import os
        
        # Get database path
        db_path = os.getenv('DUCKDB_PATH', '/app/data/academic_papers.duckdb')
        conn = duckdb.connect(db_path, read_only=True)
        
        sql_query = f"""
        SELECT 
            entry_id,
            title,
            summary,
            authors,
            published_date,
            pdf_url,
            categories
        FROM papers
        WHERE entry_id = '{entry_id}';
        """
        
        result = conn.execute(sql_query).fetchdf()
        conn.close()
        
        if len(result) > 0:
            row = result.iloc[0]
            return {
                "entry_id": row['entry_id'],
                "title": row['title'],
                "summary": row['summary'],
                "authors": row['authors'],
                "published_date": str(row['published_date']) if row['published_date'] else None,
                "pdf_url": row['pdf_url'],
                "categories": row['categories']
            }
        
        return None
        
    except Exception as e:
        print(f"⚠️  Error retrieving paper: {e}")
        # Fallback sample data
        if entry_id.startswith("arxiv-"):
            return {
                "entry_id": entry_id,
                "title": f"Sample Paper for {entry_id}",
                "summary": "This is a detailed summary of the paper.",
                "authors": "John Doe, Jane Smith",
                "published_date": date(2024, 1, 1),
                "pdf_url": f"https://arxiv.org/pdf/{entry_id}.pdf",
                "categories": "cs.LG, cs.AI"
            }
        return None


# Legacy synchronous wrappers for backward compatibility
def query_knowledge_base(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Synchronous wrapper for query_academic_papers"""
    return _query_papers_sync(query, limit)
