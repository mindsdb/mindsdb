import streamlit as st
import pandas as pd
import numpy as np

from core.kb_ops import search_kb


TYPE_OPTIONS = ["Any", "Incident", "Request", "Problem", "Change"]
PRIORITY_OPTIONS = ["Any", "high", "medium", "low"]
CATEGORY_OPTIONS = [
    "Any",
    "Technical Support",
    "Returns and Exchanges",
    "Customer Service",
    "Product Support",
    "Billing and Payments",
    "Sales and Pre-Sales",
    "IT Support",
    "Service Outages and Maintenance",
    "General Inquiry",
    "Human Resources",
]
TAG_OPTIONS = [
    "Any",
    "Urgent Issue",
    "IT Support",
    "Technical Support",
    "Returns and Exchanges",
    "Billing Issue",
    "Payment Processing",
    "Sales Inquiry",
    "Service Disruption",
    "Software Bug",
    "Customer Feedback",
    "Account Security",
    "Service Outage",
    "Product Support",
    "Network Issue",
    "Customer Service",
    "Performance Tuning",
    "Warranty Claim",
    "Technical Guidance",
    "Data Breach",
    "Incident Report",
    "Consulting Services",
    "Account Assistance",
    "Login Issue",
    "General Inquiry",
    "Refund Request",
    "Problem Resolution",
    "System Crash",
    "System Maintenance",
    "Order Issue",
    "Hardware Failure",
    "Cloud Services",
    "Product Replacement",
    "Feature Request",
    "Service Recovery",
    "Shipping Inquiry",
]


def run():
    """Main function to run the Streamlit app interface."""
    st.title("📄 Knowledge Base Search")
    st.markdown(
        "Enter a query and use the filters in the sidebar to refine your search."
    )

    st.sidebar.header("🔍 Search Filters")
    top_k_filter = st.sidebar.slider(
        "Number of results", min_value=1, max_value=10, value=3, step=1
    )
    type_filter = st.sidebar.selectbox("Type", options=TYPE_OPTIONS)
    priority_filter = st.sidebar.selectbox("Priority", options=PRIORITY_OPTIONS)
    category_filter = st.sidebar.selectbox("Category", options=CATEGORY_OPTIONS)
    tag_1_filter = st.sidebar.selectbox("Tag 1", options=TAG_OPTIONS)
    tag_2_filter = st.sidebar.selectbox("Tag 2", options=TAG_OPTIONS)

    st.subheader("Search Query")
    query = st.text_input(
        "Enter your search term(s) below:", placeholder="e.g., how to reset password"
    )

    if st.button("Search", type="primary"):
        if not query:
            st.warning("Please enter a query to search.")
            return

        filters = {
            "type": type_filter,
            "priority": priority_filter,
            "category": category_filter,
            "tag_1": tag_1_filter,
            "tag_2": tag_2_filter,
        }

        with st.spinner("Searching for relevant entries..."):
            results = search_kb(query, filters=filters, top_k=top_k_filter)
            print(results)

        st.subheader(f"🔍 Search Results for '{query}'")

        if results.empty:
            st.info("No matching KB entries found. Try adjusting your filters.")
        else:
            for _, row in results.iterrows():
                relevance_percent = f"{row['relevance']:.1%}"
                priority_color_map = {
                    "High": "red",
                    "Medium": "orange",
                    "Low": "yellow",
                }
                priority_color = priority_color_map.get(row["priority"], "gray")

                with st.container(border=True):
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.subheader(row["id"])
                    with col2:
                        st.markdown(
                            f"<p style='text-align: right; color: #9ca3af;'>Match: {relevance_percent}</p>",
                            unsafe_allow_html=True,
                        )

                    st.write(row["chunk_content"])

                    st.divider()

                    st.markdown(
                        f"""
                    <div style="display: flex; flex-wrap: wrap; gap: 10px; align-items: center; font-size: 0.9em;margin-bottom: 10px;">
                        <span><b>Category:</b> <span style="color: #4ade80;">{row['category']}</span></span>
                        <span><b>Type:</b> <span style="color: #fb92c;">{row['type']}</span></span>
                        <span><b>Priority:</b> <span style="color: {priority_color};">{row['priority']}</span></span>
                        <span style="color: #4b5563;">|</span>
                        <span style="background-color: #3b82f6; color: #ffffff; padding: 2px 8px; border-radius: 12px;">{row['tag_1']}</span>
                        <span style="background-color: #3b82f6; color: #ffffff; padding: 2px 8px; border-radius: 12px;">{row['tag_2']}</span>
                    </div>
                    """,
                        unsafe_allow_html=True,
                    )
