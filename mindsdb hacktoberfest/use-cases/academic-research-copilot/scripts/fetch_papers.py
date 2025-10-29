import arxiv
import duckdb

def fetch_and_store_papers():
    # Connect to DuckDB
    conn = duckdb.connect('data/academic_papers.duckdb')
    
    # Create table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS papers (
            id VARCHAR PRIMARY KEY,
            title VARCHAR,
            abstract VARCHAR,
            authors VARCHAR,
            published_date DATE,
            pdf_url VARCHAR,
            categories VARCHAR
        )
    """)
    
    # Fetch papers from ArXiv
    topics = [
        "machine learning",
        "deep learning",
        "natural language processing"
    ]
    
    for topic in topics:
        search = arxiv.Search(
            query=topic,
            max_results=25,
            sort_by=arxiv.SortCriterion.Relevance
        )
        
        for paper in search.results():
            conn.execute("""
                INSERT OR REPLACE INTO papers VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                paper.entry_id,
                paper.title,
                paper.summary,
                ', '.join([a.name for a in paper.authors]),
                paper.published.date(),
                paper.pdf_url,
                ', '.join(paper.categories)
            ])
            print(f"✓ Stored: {paper.title[:50]}...")
    
    conn.close()
    print(f"\n✅ Fetched and stored papers in DuckDB!")

if __name__ == "__main__":
    fetch_and_store_papers()