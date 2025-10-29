import streamlit as st
import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.ui.components.search import search_component, call_search_api
from src.ui.components.results import display_results


def main():
    """Main Streamlit application"""
    
    # Page config
    st.set_page_config(
        page_title="Academic Research Copilot",
        page_icon="🎓",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS
    st.markdown("""
        <style>
        .main-header {
            font-size: 3rem;
            font-weight: bold;
            text-align: center;
            margin-bottom: 1rem;
        }
        .subtitle {
            text-align: center;
            font-size: 1.2rem;
            color: #666;
            margin-bottom: 2rem;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.markdown('<p class="main-header">🎓 Academic Research Copilot</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="subtitle">Powered by MindsDB Knowledge Bases & FastAPI</p>', 
        unsafe_allow_html=True
    )
    
    # Sidebar
    with st.sidebar:
        st.image("https://43906340.fs1.hubspotusercontent-na1.net/hubfs/43906340/Kit/Core%20logo%20-%20full%20color/MindsDB%20Logo%20-%20Horizontal%20-%20Light%20Mode.jpg", width=200)
        st.markdown("---")
        st.markdown("### About")
        st.info(
            "This application uses MindsDB's Knowledge Bases to perform "
            "intelligent semantic search across academic papers from ArXiv. "
            "Ask questions in natural language and get relevant research papers!"
        )
        
        st.markdown("---")
        st.markdown("### API Status")
        
        # Check API health
        import httpx
        try:
            with httpx.Client() as client:
                response = client.get("http://localhost:8000/api/health", timeout=5.0)
                if response.status_code == 200:
                    st.success("✅ API is running")
                    st.caption("FastAPI backend: http://localhost:8000")
                else:
                    st.error("❌ API error")
        except Exception as e:
            st.error("❌ API not reachable")
            st.caption("Make sure FastAPI is running on port 8000")
        
        st.markdown("---")
        st.markdown("### Quick Links")
        st.markdown("- [API Docs](http://localhost:8000/api/docs)")
        st.markdown("- [ReDoc](http://localhost:8000/api/redoc)")
        st.markdown("- [MindsDB](https://mindsdb.com)")
    
    # Main content
    # Search component
    search_params = search_component()
    
    if search_params:
        with st.spinner("🔍 Searching academic papers..."):
            try:
                # Call API asynchronously
                results = asyncio.run(call_search_api(search_params))
                
                if results:
                    # Display results
                    display_results(results)
                else:
                    st.warning("No results found. Try adjusting your query or filters.")
                    
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.exception(e)
    
    # Footer
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.caption("Built for MindsDB Hacktoberfest 2025")
    with col2:
        st.caption("Powered by FastAPI & Streamlit")
    with col3:
        st.caption("🌟 Star us on GitHub!")


if __name__ == "__main__":
    main()
