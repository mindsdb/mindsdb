# KEV Insight Copilot: RAG for CISA Known Exploited Vulnerabilities

## 🌟 Problem Statement

This project solves this by providing a **Natural Language Query (NLQ)** interface to semantically search, analyze, and prioritize CISA **Known Exploited Vulnerabilities (KEV)** data *in-place*.

The goal is to allow analysts to ask complex questions (e.g., "Show me all KEVs affecting Cisco products") and receive accurate, context-aware, and explainable answers.

## 💡 Architecture and Flow

This solution uses a zero-ETL architecture, leveraging the **MindsDB Knowledge Base (KB)** for semantic RAG and hybrid query processing directly on top of the PostgreSQL data source.
<img width="859" height="546" alt="image" src="https://github.com/user-attachments/assets/d20bdc01-6e66-4c7c-a995-e1cfe22e501c" />

1.  **Data Ingestion:** CISA KEV Feed is automatically ingested via **Airbyte**.
2.  **Data Warehouse:** **PostgreSQL** stores the normalized KEV records.
3.  **AI Engine:** **MindsDB** connects to PostgreSQL, MindsDB **Knowledge Base** (`kev_kb`) is built over the KEV data, *Agent:** A MindsDB **Agent** (`kev_security_agent`) processes NL queries and generates human-readable answers.
6.  **Application:** A **Streamlit** Web App (`app.py`) serves as the user interface.



## 🛠️ Setup and Deployment

### 1. Environment Setup

Use the provided `docker-compose.yml` to launch PostgreSQL and MindsDB:
```bash
docker compose up -d
```

### 2. MindsDB Configuration (SQL)
Connect MindsDB to the PostgreSQL database and then set up the Knowledge Base and Agent using the `setup_mindsdb.sql` script.

### 3. Deploy Streamlit App
Run the Streamlit application to access the Agent via the MindsDB Python SDK:
```bash
streamlit run app.py
```

### 4. Demo Video
[DEMO VIDEO](https://youtu.be/FwLX3JqnsKQ)

### 5. Blog
[LINK BLOG POST HERE](https://maulcenter.hashnode.dev/building-a-kev-insight-copilot-with-mindsdb)
