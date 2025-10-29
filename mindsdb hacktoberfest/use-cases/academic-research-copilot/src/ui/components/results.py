import streamlit as st
from typing import List, Dict, Any


def display_results(results: List[Dict[str, Any]]):
    """
    Display search results in Streamlit interface
    
    Args:
        results: List of paper dictionaries with metadata
    """
    if not results:
        st.warning("No results found. Try a different query.")
        return
    
    st.success(f"Found {len(results)} papers")
    
    for idx, paper in enumerate(results, 1):
        with st.expander(f"📄 {idx}. {paper.get('title', 'Untitled')}", expanded=(idx == 1)):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.markdown(f"**Authors:** {paper.get('authors', 'Unknown')}")
                if paper.get('published_date'):
                    st.markdown(f"**Published:** {paper.get('published_date')}")
                if paper.get('relevance_score'):
                    st.markdown(f"**Relevance Score:** {paper.get('relevance_score'):.2f}")
            
            with col2:
                if paper.get('pdf_url'):
                    st.markdown(f"[📥 PDF Link]({paper.get('pdf_url')})")
            
            st.markdown("**Summary:**")
            st.write(paper.get('summary', 'No summary available'))
            
            st.markdown(f"**Entry ID:** `{paper.get('entry_id', 'N/A')}`")
            st.divider()


def display_paper_card(paper: Dict[str, Any]):
    """
    Display a single paper as a card in Streamlit
    
    Args:
        paper: Dictionary containing paper metadata
    """
    st.markdown(f"### 📄 {paper.get('title', 'Untitled')}")
    st.markdown(f"**Authors:** {paper.get('authors', 'Unknown')}")
    
    if paper.get('published_date'):
        st.markdown(f"**Published:** {paper.get('published_date')}")
    
    if paper.get('relevance_score'):
        st.progress(paper.get('relevance_score'))
        st.caption(f"Relevance: {paper.get('relevance_score'):.1%}")
    
    st.markdown("**Summary:**")
    st.write(paper.get('summary', 'No summary available'))
    
    if paper.get('pdf_url'):
        st.markdown(f"[📥 Download PDF]({paper.get('pdf_url')})")
