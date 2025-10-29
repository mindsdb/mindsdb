from typing import List
import requests
import duckdb

class AcademicIngestion:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def fetch_papers(self, query: str, max_results: int = 100) -> List[dict]:
        url = f"http://export.arxiv.org/api/query?search_query={query}&start=0&max_results={max_results}"
        response = requests.get(url)
        response.raise_for_status()
        return self.parse_response(response.text)

    def parse_response(self, response_text: str) -> List[dict]:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(response_text)
        papers = []
        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            paper = {
                'title': entry.find('{http://www.w3.org/2005/Atom}title').text,
                'summary': entry.find('{http://www.w3.org/2005/Atom}summary').text,
                'published': entry.find('{http://www.w3.org/2005/Atom}published').text,
                'authors': [author.find('{http://www.w3.org/2005/Atom}name').text for author in entry.findall('{http://www.w3.org/2005/Atom}author')]
            }
            papers.append(paper)
        return papers

    def store_papers(self, papers: List[dict]):
        conn = duckdb.connect(self.db_path)
        for paper in papers:
            conn.execute("""
                INSERT INTO academic_papers (title, summary, published, authors)
                VALUES (?, ?, ?, ?)
            """, (paper['title'], paper['summary'], paper['published'], ', '.join(paper['authors'])))
        conn.close()

    def ingest(self, query: str, max_results: int = 100):
        papers = self.fetch_papers(query, max_results)
        self.store_papers(papers)

# Example usage:
# ingestion = AcademicIngestion('path/to/your/database.duckdb')
# ingestion.ingest('machine learning', max_results=50)