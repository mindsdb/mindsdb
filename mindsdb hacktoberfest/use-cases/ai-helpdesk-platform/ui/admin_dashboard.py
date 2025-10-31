import streamlit as st
import pandas as pd
import os
import plotly.express as px
from core.json_store import JsonStore
from core.kb_ops import get_tickets_dataset

store = JsonStore()


def run():
    st.markdown("## 🧭 Operational Insights")

    df = get_tickets_dataset()

    # --- KPIs ---
    k1, k2, k3 = st.columns(3)
    k1.metric("Total Tickets", len(df))
    k2.metric("Distinct Categories", df["category"].nunique())
    k3.metric("Unique Tags", df["tag_1"].nunique() + df["tag_2"].nunique())

    st.markdown("---")

    # --- Layout Grid ---
    c1, c2 = st.columns(2)
    c3, c4 = st.columns(2)

    # Priority Pie
    with c1:
        prio_counts = df["priority"].value_counts().reset_index()
        prio_counts.columns = ["Priority", "Count"]
        fig1 = px.pie(
            prio_counts,
            names="Priority",
            values="Count",
            color="Priority",
            title="Priority Breakdown",
            template="plotly_dark",
            color_discrete_sequence=px.colors.sequential.Tealgrn,
        )
        st.plotly_chart(fig1, use_container_width=True)

    # Ticket Type Bar
    with c2:
        type_counts = df["type"].value_counts().reset_index()
        type_counts.columns = ["Type", "Count"]
        fig2 = px.bar(
            type_counts,
            x="Type",
            y="Count",
            color="Type",
            title="Ticket Type Distribution",
            template="plotly_dark",
            color_discrete_sequence=px.colors.sequential.Mint,
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Category Bar
    with c3:
        cat_counts = df["category"].value_counts().reset_index().head(10)
        cat_counts.columns = ["Category", "Count"]
        fig3 = px.bar(
            cat_counts,
            y="Category",
            x="Count",
            orientation="h",
            color="Category",
            title="Top 10 Categories",
            template="plotly_dark",
            color_discrete_sequence=px.colors.sequential.Viridis,
        )
        st.plotly_chart(fig3, use_container_width=True)

    # Tags Pie (tag_1 + tag_2 combined)
    with c4:
        tag_combo = (
            pd.concat([df["tag_1"], df["tag_2"]]).value_counts().reset_index().head(8)
        )
        tag_combo.columns = ["Tag", "Count"]
        fig4 = px.pie(
            tag_combo,
            names="Tag",
            values="Count",
            color="Tag",
            title="Top Tags",
            template="plotly_dark",
            color_discrete_sequence=px.colors.sequential.Magenta,
        )
        st.plotly_chart(fig4, use_container_width=True)
