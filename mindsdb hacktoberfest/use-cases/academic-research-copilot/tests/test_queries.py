import pytest
from src.knowledge_base.queries import query_academic_papers

def test_query_academic_papers():
    # Test case 1: Query for a specific paper by title
    title_query = "A Comprehensive Study on AI"
    expected_result = {
        "title": "A Comprehensive Study on AI",
        "authors": ["John Doe", "Jane Smith"],
        "abstract": "This paper discusses various aspects of AI...",
        "year": 2023
    }
    result = query_academic_papers(title=title_query)
    assert result == expected_result, f"Expected {expected_result}, but got {result}"

    # Test case 2: Query for papers by a specific author
    author_query = "John Doe"
    expected_results = [
        {
            "title": "A Comprehensive Study on AI",
            "authors": ["John Doe", "Jane Smith"],
            "abstract": "This paper discusses various aspects of AI...",
            "year": 2023
        },
        {
            "title": "AI in Healthcare",
            "authors": ["John Doe"],
            "abstract": "This paper explores the applications of AI in healthcare...",
            "year": 2022
        }
    ]
    results = query_academic_papers(author=author_query)
    assert results == expected_results, f"Expected {expected_results}, but got {results}"

    # Test case 3: Query for papers published in a specific year
    year_query = 2023
    expected_results_year = [
        {
            "title": "A Comprehensive Study on AI",
            "authors": ["John Doe", "Jane Smith"],
            "abstract": "This paper discusses various aspects of AI...",
            "year": 2023
        }
    ]
    results_year = query_academic_papers(year=year_query)
    assert results_year == expected_results_year, f"Expected {expected_results_year}, but got {results_year}"

    # Test case 4: Query with no results
    no_results_query = "Nonexistent Paper Title"
    results_no = query_academic_papers(title=no_results_query)
    assert results_no == [], f"Expected no results, but got {results_no}"