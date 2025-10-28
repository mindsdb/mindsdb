import streamlit as st
import mindsdb_sdk
import pandas as pd
import time
import altair as alt

# --- UI Config ---
st.set_page_config(page_title="KEV Security Analytics", page_icon="🛡️", layout="wide")

# Sidebar Menu
menu = st.sidebar.radio("📌 Navigation", ["Dashboard", "KEV Insight Copilot"])

st.sidebar.title("KEV Security Agent")
server_url = st.sidebar.text_input("MindsDB Server URL", "http://127.0.0.1:47334")
project_name = st.sidebar.text_input("Project Name", "mindsdb")
agent_name = st.sidebar.text_input("Agent/Table Name", "kev_security_agent")

# --- Connect to MindsDB ---
@st.cache_resource
def connect_mindsdb(url):
    """Establishes connection to MindsDB server."""
    try:
        return mindsdb_sdk.connect(url)
    except Exception:
        return None

server = connect_mindsdb(server_url)

if server is None:
    st.error("❌ Failed to connect to MindsDB. Please check the URL.")
    st.stop()

# Get the MindsDB project
try:
    project = server.get_project(project_name)
except Exception as e:
    st.error(f"❌ Failed to get MindsDB project: {e}")
    st.stop()


# ============================================================
# ✅ PAGE 1 – DASHBOARD
# ============================================================
if menu == "Dashboard":
    st.title("📊 CISA KEV Catalog Vulnerability Dashboard")

    query_str = """
    SELECT
        *
    FROM cyber_postgres.public.cisa_known_exploited;
    """
    
    # --- Data Fetching ---
    try:
        df = project.query(query_str).fetch()
    except Exception as e:
        st.error(f"Query Error: {e}")
        st.stop()
    
    # --- Data Cleaning and Preprocessing ---
    # Rename columns for clarity
    df = df.rename(columns={
    "dateAdded": "date_added",
    "vendorProject": "vendor",
    "cveID": "cve_id",
    "knownRansomwareCampaignUse": "ransomware_use"
    })

    # Convert date to datetime and drop missing dates
    df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce")
    df.dropna(subset=['date_added'], inplace=True) 

    # --- KPIs ---
    total_vuln = len(df)
    unique_vendor = df["vendor"].nunique()
    ransomware_related = df[df["ransomware_use"] == "Yes"].shape[0]

    col1, col2, col3 = st.columns(3)
    col1.metric("Total KEVs Listed", total_vuln)
    col2.metric("Unique Vendors Affected", unique_vendor)
    col3.metric("Ransomware-Related CVEs", ransomware_related)

    # --- Monthly Submission Trend Plot ---
    st.markdown("### 📈 Monthly Submission Trend of KEVs")
    
    # Group by month and convert index to DatetimeIndex for correct plotting
    monthly_trend_series = df.groupby(df["date_added"].dt.to_period("M")).size()
    monthly_trend_series.index = monthly_trend_series.index.to_timestamp() 
    
    # Convert to DataFrame for explicit x/y mapping in st.line_chart (Altair)
    monthly_trend_df = monthly_trend_series.rename("Count").reset_index()
    monthly_trend_df = monthly_trend_df.rename(columns={'date_added': 'Date'})

    # Use st.line_chart for interactive time series plot
    st.line_chart(monthly_trend_df, x='Date', y='Count', color="#1f77b4") 


    # --- Other Charts (using Altair for color and interactivity) ---
    colA, colB = st.columns(2)

    with colA:
        # Top 10 Vendor Distribution
        st.markdown("### 🏢 Top 10 Frequency Distribution by Vendor")
        top_vendors = df["vendor"].value_counts().head(10).reset_index()
        top_vendors.columns = ['Vendor', 'Count']

        # Altair chart: color by category
        chart_vendors = alt.Chart(top_vendors).mark_bar().encode(
            x=alt.X('Count', title='Number of KEVs'),
            y=alt.Y('Vendor', sort='-x', title='Vendor'),
            color=alt.Color('Vendor', title='Vendor', legend=None), 
            tooltip=['Vendor', 'Count']
        ).properties(
            title='Top 10 Frequency Distribution by Vendor'
        ).interactive()
        st.altair_chart(chart_vendors, use_container_width=True)

    with colB:
        # Top 10 Product Concentration
        st.markdown("### 🛠️ Top 10 Vulnerability Concentration by Product")
        top_products = df["product"].value_counts().head(10).reset_index()
        top_products.columns = ['Product', 'Count']

        # Altair chart: color by category
        chart_products = alt.Chart(top_products).mark_bar().encode(
            x=alt.X('Count', title='Number of KEVs'),
            y=alt.Y('Product', sort='-x', title='Product'),
            color=alt.Color('Product', title='Product', legend=None), 
            tooltip=['Product', 'Count']
        ).properties(
            title='Top 10 Vulnerability Concentration by Product'
        ).interactive()
        st.altair_chart(chart_products, use_container_width=True)

    # Ransomware Campaign Distribution
    st.markdown("### 💥 Distribution of Vulnerabilities Used in Ransomware Campaigns")
    ransomware_dist = df["ransomware_use"].value_counts().reset_index()
    ransomware_dist.columns = ['Ransomware Use', 'Count'] 

    # Altair chart: custom colors for 'Yes/No/Unknown'
    chart_ransomware = alt.Chart(ransomware_dist).mark_bar().encode(
        x=alt.X('Ransomware Use', title='Used in Ransomware Campaign'),
        y=alt.Y('Count', title='Number of KEVs'),
        color=alt.Color('Ransomware Use', 
                        scale=alt.Scale(domain=['Yes', 'No', 'Unknown'], range=['#e45757', '#57a44c', '#7f7f7f']), 
                        title='Ransomware Use'),
        tooltip=['Ransomware Use', 'Count']
    ).properties(
        title='Distribution of Vulnerabilities Used in Ransomware Campaigns'
    ).interactive()
    st.altair_chart(chart_ransomware, use_container_width=True)


    # Prevalent CWE Weakness Frequencies
    st.markdown("### 🧩 Top 15 Prevalent CWE Weakness Frequencies")
    expanded_cwes = df["cwes"].explode().dropna()
    cwe_counts = expanded_cwes.value_counts().head(15).reset_index()
    cwe_counts.columns = ['CWE ID', 'Count'] 

    # Altair chart: color by CWE ID
    chart_cwes = alt.Chart(cwe_counts).mark_bar().encode(
        x=alt.X('Count', title='Frequency'),
        y=alt.Y('CWE ID', sort='-x', title='CWE Weakness'),
        color=alt.Color('CWE ID', title='CWE ID', legend=None), 
        tooltip=['CWE ID', 'Count']
    ).properties(
        title='Top 15 Prevalent CWE Weakness Frequencies'
    ).interactive()
    st.altair_chart(chart_cwes, use_container_width=True)


    # --- Data Preview ---
    st.markdown("---")
    st.subheader("🔎 KEV Core Data Preview (First 50 Entries)")
    
    # Define the core columns explicitly to exclude metadata
    core_columns = [
        "cve_id",         
        "vendor",         
        "product",
        "vulnerabilityName", 
        "date_added",       
        "requiredAction",
        "dueDate",
        "ransomware_use", 
        "cwes"
    ]
    
    # Select only the core columns and the first 50 rows
    try:
        df_preview = df[core_columns].head(50)
        st.dataframe(df_preview, use_container_width=True)
    except KeyError as e:
        st.error(f"Configuration Error: One or more specified columns are missing after renaming. Missing columns: {e}")
        st.dataframe(df.head(5), use_container_width=True) 


# ============================================================
# ✅ PAGE 2 – KEV Insight Copilot (Chat Agent)
# ============================================================
if menu == "KEV Insight Copilot":

    st.title("🤖 KEV Insight Copilot")
    st.markdown("Ask anything related to known exploited vulnerabilities.")

    # Init State for chat history
    if "processing" not in st.session_state:
        st.session_state.processing = False
    if "last_answer" not in st.session_state:
        st.session_state.last_answer = None
    if "history" not in st.session_state:
        st.session_state.history = []

    question = st.text_input("Your Question:")
    ask_btn = st.button("Ask", disabled=st.session_state.processing or len(question.strip()) == 0)

    # Send Query to MindsDB Agent
    if ask_btn:
        st.session_state.processing = True
        st.session_state.last_answer = None
        st.rerun()

    if st.session_state.processing:
        with st.spinner("Analyzing vulnerability database…"):
            try:
                # Query the MindsDB agent table
                query_str = f"SELECT answer FROM {agent_name} WHERE question = '{question}';"
                result = project.query(query_str).fetch()

                answer = result.iloc[0]["answer"] if not result.empty else "⚠️ No answer available."
                st.session_state.last_answer = answer

                # Update chat history
                st.session_state.history.append({"role": "user", "text": question})
                st.session_state.history.append({"role": "agent", "text": answer})

            except Exception as e:
                st.session_state.last_answer = f"❌ Error: {str(e)}"

            time.sleep(0.5)
            st.session_state.processing = False
            st.rerun()

    # Display Chat History
    for msg in reversed(st.session_state.history[-8:]):
        if msg["role"] == "user":
            st.markdown(f"🧑 **You:** {msg['text']}")
        else:
            st.markdown(f"🤖 **Agent:** {msg['text']}")

    if st.session_state.last_answer and not st.session_state.processing:
        st.success("✅ Done")


st.markdown("---")
st.caption("Powered by MindsDB • Threat Intelligence Automation")
