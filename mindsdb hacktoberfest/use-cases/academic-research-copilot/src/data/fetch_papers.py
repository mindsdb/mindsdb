"""
Data ingestion module for fetching academic papers from ArXiv API
"""
import arxiv
import duckdb
from typing import List, Dict, Any, Optional
import os
import time
from datetime import datetime


def fetch_arxiv_papers(
    query: str = "machine learning",
    max_results: int = 100,
    sort_by: arxiv.SortCriterion = arxiv.SortCriterion.SubmittedDate,
    client: Optional[arxiv.Client] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch papers from ArXiv API
    
    Args:
        query: Search query for ArXiv
        max_results: Maximum number of papers to fetch
        sort_by: Sort criterion for results
        
    Returns:
        List of paper dictionaries
    """
    print(f"📚 Fetching {max_results} papers on '{query}' from ArXiv...")

    # Build a polite client with throttling and retries
    if client is None:
        delay_seconds = int(os.getenv("ARXIV_DELAY_SECONDS", "3"))
        page_size = min(int(os.getenv("ARXIV_PAGE_SIZE", "50")), max_results)
        num_retries = int(os.getenv("ARXIV_NUM_RETRIES", "5"))
        client = arxiv.Client(
            page_size=page_size,
            delay_seconds=delay_seconds,
            num_retries=num_retries,
        )

    search = arxiv.Search(
        query=query,
        max_results=max_results,
        sort_by=sort_by,
    )

    papers = []
    try:
        for result in client.results(search):
            paper = {
                "entry_id": result.entry_id,
                "title": result.title,
                "summary": result.summary.replace("\n", " "),
                "authors": ", ".join([author.name for author in result.authors]),
                "published_date": result.published.date(),
                "pdf_url": result.pdf_url,
                "categories": ", ".join(result.categories),
            }
            papers.append(paper)
            print(f"  ✓ {paper['title'][:60]}...")
    except arxiv.UnexpectedEmptyPageError as e:
        # arXiv occasionally returns an empty page when paginating; keep what we have
        print(f"  ⚠️  Received empty page from arXiv, stopping early: {e}")
    except Exception as e:
        print(f"  ⚠️  Error while fetching results: {e}")
    
    print(f"✅ Fetched {len(papers)} papers successfully!\n")
    return papers


def create_duckdb_table(db_path: str = "data/academic_papers.duckdb"):
    """
    Create DuckDB database and papers table
    
    Args:
        db_path: Path to DuckDB database file
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = duckdb.connect(database=db_path)
    
    # Create table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS papers (
            entry_id VARCHAR PRIMARY KEY,
            title VARCHAR,
            summary TEXT,
            authors VARCHAR,
            published_date DATE,
            pdf_url VARCHAR,
            categories VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    print(f"✅ DuckDB table created at: {db_path}\n")
    conn.close()
    return db_path


def insert_papers_to_duckdb(
    papers: List[Dict[str, Any]], 
    db_path: str = "data/academic_papers.duckdb"
):
    """
    Insert papers into DuckDB
    
    Args:
        papers: List of paper dictionaries
        db_path: Path to DuckDB database
    """
    conn = duckdb.connect(database=db_path)
    
    print(f"💾 Inserting {len(papers)} papers into DuckDB...")
    
    inserted = 0
    for paper in papers:
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO papers 
                (entry_id, title, summary, authors, published_date, pdf_url, categories)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    paper["entry_id"],
                    paper["title"],
                    paper["summary"],
                    paper["authors"],
                    paper["published_date"],
                    paper["pdf_url"],
                    paper["categories"]
                )
            )
            inserted += 1
        except Exception as e:
            print(f"  ⚠️  Error inserting paper {paper['entry_id']}: {e}")
    
    # Verify insertion
    count = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    print(f"✅ Inserted {inserted} papers. Total papers in database: {count}\n")
    
    conn.close()


def ingest_papers_pipeline(
    topics: List[str] = None,
    papers_per_topic: int = 50,
    db_path: str = "data/academic_papers.duckdb",
    delay_between_topics: float = 1.5,
) -> str:
    """
    Complete pipeline to fetch and store papers
    
    Args:
        topics: List of research topics to search
        papers_per_topic: Number of papers per topic
        db_path: Path to DuckDB database
        
    Returns:
        Path to created database
    """
    if topics is None:
        topics = [
            "machine learning",
            "deep learning",
            "natural language processing",
            "computer vision",
            "reinforcement learning"
        ]
    
    print("=" * 60)
    print("📖 ARXIV PAPER INGESTION PIPELINE")
    print("=" * 60 + "\n")
    
    # Create database and table
    create_duckdb_table(db_path)
    
    # Set up a single shared arXiv client for the whole run (polite defaults)
    delay_seconds = int(os.getenv("ARXIV_DELAY_SECONDS", "3"))
    page_size = min(int(os.getenv("ARXIV_PAGE_SIZE", "50")), papers_per_topic)
    num_retries = int(os.getenv("ARXIV_NUM_RETRIES", "5"))
    client = arxiv.Client(
        page_size=page_size,
        delay_seconds=delay_seconds,
        num_retries=num_retries,
    )

    # Fetch and insert papers for each topic
    all_papers = []
    for topic in topics:
        papers = fetch_arxiv_papers(
            query=topic,
            max_results=papers_per_topic,
            client=client,
        )
        all_papers.extend(papers)
        insert_papers_to_duckdb(papers, db_path)
        # Be polite to the API between topics
        if delay_between_topics > 0:
            time.sleep(delay_between_topics)
    
    print("=" * 60)
    print(f"🎉 PIPELINE COMPLETE! Total papers: {len(all_papers)}")
    print("=" * 60 + "\n")
    
    return db_path


if __name__ == "__main__":
    # Run the ingestion pipeline
    db_path = ingest_papers_pipeline(
        topics=[
            "federated learning",
            "transformer models",
            "graph neural networks",
            "meta learning",
            "few-shot learning"
        ],
        papers_per_topic=30
    )
    print(f"Database created at: {db_path}")
