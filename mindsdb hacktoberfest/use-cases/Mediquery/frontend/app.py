import os
import streamlit as st
import requests
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="MediQuery", layout="wide")

BACKEND_URL = st.secrets.get("BACKEND_URL", os.getenv("BACKEND_URL", "http://localhost:8000"))

st.title("🧠 MediQuery — Medical AI Analytics")

with st.expander("⚙️ Connection Settings"):
    st.write("Backend URL:", BACKEND_URL)
    if st.button("Check backend connection"):
        try:
            r = requests.get(f"{BACKEND_URL}/health", timeout=5)
            st.success(r.json())
        except Exception as e:
            st.error(f"Cannot reach backend: {e}")

# ---------- Layout ----------
col1, col2 = st.columns([1, 2])

# ---------- LEFT COLUMN ----------
with col1:
    st.subheader("💬 Ask the Agent")
    agent_name_input = st.text_input("Agent name", value="department_analysis")
    question = st.text_area("Your question", value="What is the average patient satisfaction by department?")

    if st.button("Ask Agent"):
        agent_name = agent_name_input.strip()
        payload = {"agent_name": agent_name, "question": question.strip()}

        with st.spinner("🤖 Querying agent..."):
            try:
                r = requests.post(f"{BACKEND_URL}/ask_agent", json=payload, timeout=60)

                if not r.ok:
                    st.error(f"Agent query failed: {r.text}")
                else:
                    data = r.json()

                    # ---- Clean & simplify the raw response ----
                    mindsdb_response = data.get("mindsdb_response", {})

                    clean_text = ""
                    try:
                        # Try to get "data" content (most likely answer)
                        if "data" in mindsdb_response:
                            d = mindsdb_response["data"]
                            if isinstance(d, list) and len(d) > 0:
                                first_item = d[0]
                                # If it's a list of lists, extract text
                                if isinstance(first_item, list) and len(first_item) > 0:
                                    clean_text = str(first_item[0]).strip()
                                elif isinstance(first_item, dict):
                                    clean_text = next(iter(first_item.values()), "")
                                else:
                                    clean_text = str(first_item)
                            else:
                                clean_text = str(d)
                        # Fallback if "answer" directly in response
                        elif "answer" in mindsdb_response:
                            clean_text = mindsdb_response["answer"]
                        else:
                            clean_text = str(mindsdb_response)
                    except Exception:
                        clean_text = str(mindsdb_response)

                    # ---- Display clean, formatted output ----
                    st.subheader("✅ Agent Answer")
                    st.markdown(clean_text)

                    # Keep the SQL visible but tucked away
                    with st.expander("🧩 SQL Used (optional view)"):
                        st.code(data.get("sql", "No SQL returned"))

                    # Keep the raw response hidden unless expanded
                    with st.expander("🧾 Full Raw Response (debug only)"):
                        st.json(mindsdb_response)

            except Exception as e:
                st.error(f"Agent request failed: {e}")

    # ---------- KB SEARCH ----------
    st.markdown("---")
    st.subheader("🔍 Knowledge Base Search")
    kb_full_name = st.text_input("KB full name", value="project.patient_reviews_kb")
    query_text = st.text_input("Search query", value="excellent doctor communication and clean facilities")
    limit = st.slider("Limit results", 1, 50, 10)

    st.markdown("*Optional metadata filters*")
    dept = st.text_input("department", value="")
    patient_type = st.text_input("patient_type", value="")
    filters = {}
    if dept.strip():
        filters["department"] = dept.strip()
    if patient_type.strip():
        filters["patient_type"] = patient_type.strip()

    if st.button("Search KB"):
        payload = {
            "kb_full_name": kb_full_name.strip(),
            "query_text": query_text.strip(),
            "limit": limit,
            "metadata_filters": filters or None,
        }
        with st.spinner("🔎 Searching KB..."):
            try:
                r = requests.post(f"{BACKEND_URL}/kb_search", json=payload, timeout=30)
                if not r.ok:
                    st.error(f"KB search failed: {r.text}")
                else:
                    data = r.json()
                    mindsdb_response = data.get("mindsdb_response", {})

                    # Try to extract clean, tabular data
                    try:
                        rows = mindsdb_response.get("data", {}).get("rows", [])
                        columns = mindsdb_response.get("data", {}).get("columns", [])
                        if rows and columns:
                            df = pd.DataFrame(rows, columns=columns)
                            st.subheader("📊 Results Table")
                            st.dataframe(df)
                            if "overall_rating" in df.columns:
                                st.bar_chart(df["overall_rating"].value_counts().sort_index())
                        else:
                            # Display fallback clean text
                            st.markdown("### Cleaned Response")
                            st.markdown(str(mindsdb_response.get("data", "")))
                    except Exception as ex:
                        st.warning(f"Could not parse KB data: {ex}")

                    with st.expander("🧩 SQL Used"):
                        st.code(data.get("sql", "No SQL returned"))
                    with st.expander("🧾 Full Raw Response"):
                        st.json(mindsdb_response)

            except Exception as e:
                st.error(f"KB request failed: {e}")

# ---------- RIGHT COLUMN ----------
with col2:
    st.subheader("📈 Insights / Visualizations")
    st.markdown("Run a KB Search to generate charts and tables here.")
    st.info("💡 Tip: Filter by department to visualize satisfaction or treatment effectiveness.")
