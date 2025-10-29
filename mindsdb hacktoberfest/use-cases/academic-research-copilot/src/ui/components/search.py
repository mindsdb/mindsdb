import streamlit as st
import httpx
from typing import Optional, Dict, Any


def search_component() -> Optional[str]:
    """
    Streamlit search component for user input
    
    Returns:
        str: User's search query or None
    """
    st.markdown("## 🔍 Search Academic Papers")
    
    # Search input
    query = st.text_input(
        "Enter your research question:",
        placeholder="e.g., 'What are the latest advances in federated learning?'",
        help="Ask natural language questions about academic research"
    )
    
    # Search options
    col1, col2, col3 = st.columns(3)
    
    with col1:
        search_type = st.selectbox(
            "Search Type",
            ["Basic", "Semantic", "Hybrid"],
            help="Choose the type of search to perform"
        )
    
    with col2:
        limit = st.slider("Max Results", min_value=5, max_value=50, value=10, step=5)
    
    with col3:
        if search_type == "Semantic":
            threshold = st.slider("Similarity Threshold", min_value=0.0, max_value=1.0, value=0.7, step=0.05)
        else:
            threshold = None
    
    # Advanced filters for hybrid search
    metadata_filters = None
    if search_type == "Hybrid":
        with st.expander("🔧 Advanced Filters"):
            col_a, col_b = st.columns(2)
            with col_a:
                author_filter = st.text_input("Author name (optional)")
            with col_b:
                year_filter = st.number_input("Published after year (optional)", min_value=1900, max_value=2025, value=2020)
            
            if author_filter or year_filter:
                metadata_filters = {}
                if author_filter:
                    metadata_filters["authors"] = author_filter
                if year_filter:
                    metadata_filters["year"] = year_filter
    
    # Search button
    if st.button("🚀 Search", type="primary", use_container_width=True):
        if not query:
            st.error("Please enter a search query")
            return None
        
        return {
            "query": query,
            "search_type": search_type,
            "limit": limit,
            "threshold": threshold,
            "metadata_filters": metadata_filters
        }
    
    return None


async def call_search_api(search_params: Dict[str, Any]) -> list:
    """
    Call the FastAPI backend to perform search
    
    Args:
        search_params: Dictionary containing search parameters
        
    Returns:
        list: Search results from the API
    """
    base_url = "http://localhost:8000/api"
    
    try:
        async with httpx.AsyncClient() as client:
            search_type = search_params.get("search_type", "Basic")
            
            if search_type == "Basic":
                endpoint = f"{base_url}/search"
                payload = {
                    "query": search_params["query"],
                    "limit": search_params["limit"]
                }
            elif search_type == "Semantic":
                endpoint = f"{base_url}/search/semantic"
                payload = {
                    "query": search_params["query"],
                    "threshold": search_params["threshold"]
                }
            elif search_type == "Hybrid":
                endpoint = f"{base_url}/search/hybrid"
                payload = {
                    "query": search_params["query"],
                    "metadata_filters": search_params.get("metadata_filters"),
                    "limit": search_params["limit"]
                }
            
            response = await client.post(endpoint, json=payload, timeout=30.0)
            response.raise_for_status()
            
            return response.json().get("results", [])
            
    except httpx.HTTPError as e:
        st.error(f"API Error: {str(e)}")
        return []
    except Exception as e:
        st.error(f"Unexpected error: {str(e)}")
        return []
