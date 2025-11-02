## Problem statement
This project builds an evidence-first financial reporting assistant that analyzes public market signals and structured analyses to produce concise, verifiable outputs for equity research and trading workflows. It solves the problem of synthesizing noisy community signals, news, and historical analyses into a structured summary, valuation inputs, evidence-backed recommendations, and machine-readable JSON suitable for downstream automation and dashboards.

Primary use cases:
- Rapid company-level sentiment and valuation summaries for analysts and traders
- Automated ingestion of analysis results into a central database for retrieval and search
- Evidence-backed Q&A via an agent that cites knowledge base chunks for auditability

## Architecture
High-level components:
- Data ingestion: web crawler populates `my_web.crawler` with scraped content (news, transcripts, forum posts).
- MindsDB project: hosts the LLM model and enables SQL-style queries against the model.
- Knowledge Bases: embeddings and reranking indexes built inside MindsDB for fast evidence retrieval.
- Postgres: stores structured analysis results in `analysis_results`.
- Agent layer: MindsDB agent `financial_reporting_agent` that uses KBs to answer queries and produce JSON outputs.
- UI / API: Flask app (templates/app.html) exposing endpoints `/chat` and `/stream_chat` for interactive usage.

Flow:
1. Ingested content → crawler
2. Periodic analysis pipeline queries the model and writes JSON results to Postgres
3. Knowledge bases created from Postgres and crawler content
4. Agent uses KBs to answer user queries with provenance
5. UI queries the agent and renders markdown / JSON

## Knowledge Base schema
Example KB configuration (conceptual):

- id_column: `id` or `url`
- content_columns:
  - `summary`
  - `company_summary`
  - `news_summary`
  - `bullish_reasons`
  - `bearish_reasons`
  - `speculative_posts`
  - `valuation_comment`
  - `news_bullish_factors`
  - `news_bearish_factors`
  - `action_points`
  - `trading_recommendations`
- metadata_columns:
  - `ticker`
  - `analysis_date`
  - `overall_sentiment`
  - `confidence_score`
  - `credibility`
  - `pe_ratio`
  - `eps`
  - `market_cap`
  - `dividend_yield`
  - `price_to_book`
  - `week_52_high`
  - `week_52_low`
  - `valuation_status`
  - `created_at`
- preprocessing:
  - text chunking with chunk_size: 800, chunk_overlap: 150

Best practice: store numeric fields in metadata for programmatic valuation and store human-readable fields in content columns for evidence extraction.

## SQL examples
Note: replace placeholder credentials and API keys with your secure values.

Create model (example):
```sql
CREATE MODEL mindsdb.financial_analyzer
PREDICT answer
USING
  engine = 'google_gemini_engine',
  model_name = 'gemini-2.5-flash',
  api_key = '<YOUR_GOOGLE_API_KEY>',
  question_column = 'question';
```

Register Postgres with MindsDB:
```sql
CREATE DATABASE postgresql_conn
WITH ENGINE = 'postgres'
PARAMETERS = {
  "host": "host.docker.internal",
  "port": 5432,
  "database": "stock_analysis",
  "user": "your_user",
  "schema": "public",
  "password": "your_password"
};
```

Create a knowledge base (conceptual):
```sql
CREATE KNOWLEDGE_BASE equity_analysis_kb
USING
  embedding_model = {...},
  reranking_model = {...},
  content_columns = ['summary','company_summary','news_summary', ...],
  metadata_columns = ['ticker','analysis_date','overall_sentiment', ...],
  id_column = 'id',
  preprocessing = { "text_chunking_config": { "chunk_size": 800, "chunk_overlap": 150 } };
```

Populate KB from Postgres:
```sql
INSERT INTO equity_analysis_kb
SELECT * FROM postgresql_conn.analysis_results WHERE id BETWEEN 1 AND 1000;
```

Query agent (simple usage via MindsDB agent):
```sql
SELECT answer
FROM mindsdb.financial_analyzer
WHERE question = 'Give JSON summary for TCS using latest KB evidence';
```

Search KB example:
```sql
SELECT chunk_content, relevance
FROM equity_analysis_kb
WHERE content = 'Which stocks will benefit from India growth?'
ORDER BY relevance DESC
LIMIT 5;
```

## Metrics and monitoring
Track these core metrics to validate pipeline health and answer quality:

- Pipeline metrics
  - total_processed: count of stocks analyzed
  - success_count / error_count / skip_count
  - average_latency_per_stock (seconds)
  - ingestion_rate (items/min)

- Model / agent quality metrics
  - response_rate: percent of successful model responses
  - JSON_parsing_failure_rate
  - average_token_usage (if available)

- Knowledge base & evidence metrics
  - average_relevance_score per query
  - % queries with combined evidence_relevance >= 0.2
  - KB_coverage: fraction of tickers with at least one KB record
  - confidence_distribution: histogram of confidence_score values (Low/Medium/High)

- Operational/observability
  - API latency (95th percentile) for `/chat` and `/stream_chat`
  - Agent fallback rate (times hybrid search invoked)
  - PostgreSQL insertion success/failure rate

Suggested alert thresholds:
- JSON_parsing_failure_rate > 5% → investigate model prompt or output sanitization
- Agent fallback rate > 20% → expand KB content or refresh embeddings
- API 95p latency > 2s → scale UI/API or review model execution times

## Notes
- Keep secrets (API keys, DB passwords) in a .env file and ensure .env is in .gitignore.
- Validate model outputs programmatically (schema checks) before inserting into Postgres.
- Use provenance requirements from the agent prompt: always include KB id/url and relevance for traceability.

```// filepath: /Users/bseetharaman/Desktop/AI_Apps_25/mdb_hackathon/README.md
# Financial Reporting Assistant — README

## Problem statement
This project builds an evidence-first financial reporting assistant that analyzes public market signals and structured analyses to produce concise, verifiable outputs for equity research and trading workflows. It solves the problem of synthesizing noisy community signals, news, and historical analyses into a structured summary, valuation inputs, evidence-backed recommendations, and machine-readable JSON suitable for downstream automation and dashboards.

Primary use cases:
- Rapid company-level sentiment and valuation summaries for analysts and traders
- Automated ingestion of analysis results into a central database for retrieval and search
- Evidence-backed Q&A via an agent that cites knowledge base chunks for auditability

## Architecture
High-level components:
- Data ingestion: web crawler populates `my_web.crawler` with scraped content (news, transcripts, forum posts).
- MindsDB project: hosts the LLM model and enables SQL-style queries against the model.
- Knowledge Bases: embeddings and reranking indexes built inside MindsDB for fast evidence retrieval.
- Postgres: stores structured analysis results in `analysis_results`.
- Agent layer: MindsDB agent `financial_reporting_agent` that uses KBs to answer queries and produce JSON outputs.
- UI / API: Flask app (templates/app.html) exposing endpoints `/chat` and `/stream_chat` for interactive usage.

Flow:
1. Ingested content → crawler
2. Periodic analysis pipeline queries the model and writes JSON results to Postgres
3. Knowledge bases created from Postgres and crawler content
4. Agent uses KBs to answer user queries with provenance
5. UI queries the agent and renders markdown / JSON

## Knowledge Base schema
Example KB configuration (conceptual):

- id_column: `id` or `url`
- content_columns:
  - `summary`
  - `company_summary`
  - `news_summary`
  - `bullish_reasons`
  - `bearish_reasons`
  - `speculative_posts`
  - `valuation_comment`
  - `news_bullish_factors`
  - `news_bearish_factors`
  - `action_points`
  - `trading_recommendations`
- metadata_columns:
  - `ticker`
  - `analysis_date`
  - `overall_sentiment`
  - `confidence_score`
  - `credibility`
  - `pe_ratio`
  - `eps`
  - `market_cap`
  - `dividend_yield`
  - `price_to_book`
  - `week_52_high`
  - `week_52_low`
  - `valuation_status`
  - `created_at`
- preprocessing:
  - text chunking with chunk_size: 800, chunk_overlap: 150

Best practice: store numeric fields in metadata for programmatic valuation and store human-readable fields in content columns for evidence extraction.

## SQL examples
Note: replace placeholder credentials and API keys with your secure values.

Create model (example):
```sql
CREATE MODEL mindsdb.financial_analyzer
PREDICT answer
USING
  engine = 'google_gemini_engine',
  model_name = 'gemini-2.5-flash',
  api_key = '<YOUR_GOOGLE_API_KEY>',
  question_column = 'question';
```

Register Postgres with MindsDB:
```sql
CREATE DATABASE postgresql_conn
WITH ENGINE = 'postgres'
PARAMETERS = {
  "host": "host.docker.internal",
  "port": 5432,
  "database": "stock_analysis",
  "user": "your_user",
  "schema": "public",
  "password": "your_password"
};
```

Create a knowledge base (conceptual):
```sql
CREATE KNOWLEDGE_BASE equity_analysis_kb
USING
  embedding_model = {...},
  reranking_model = {...},
  content_columns = ['summary','company_summary','news_summary', ...],
  metadata_columns = ['ticker','analysis_date','overall_sentiment', ...],
  id_column = 'id',
  preprocessing = { "text_chunking_config": { "chunk_size": 800, "chunk_overlap": 150 } };
```

Populate KB from Postgres:
```sql
INSERT INTO equity_analysis_kb
SELECT * FROM postgresql_conn.analysis_results WHERE id BETWEEN 1 AND 1000;
```

Query agent (simple usage via MindsDB agent):
```sql
SELECT answer
FROM mindsdb.financial_analyzer
WHERE question = 'Give JSON summary for TCS using latest KB evidence';
```

Search KB example:
```sql
SELECT chunk_content, relevance
FROM equity_analysis_kb
WHERE content = 'Which stocks will benefit from India growth?'
ORDER BY relevance DESC
LIMIT 5;
```

## Metrics and monitoring
Track these core metrics to validate pipeline health and answer quality:

- Pipeline metrics
  - total_processed: count of stocks analyzed
  - success_count / error_count / skip_count
  - average_latency_per_stock (seconds)
  - ingestion_rate (items/min)

- Model / agent quality metrics
  - response_rate: percent of successful model responses
  - JSON_parsing_failure_rate
  - average_token_usage (if available)

- Knowledge base & evidence metrics
  - average_relevance_score per query
  - % queries with combined evidence_relevance >= 0.2
  - KB_coverage: fraction of tickers with at least one KB record
  - confidence_distribution: histogram of confidence_score values (Low/Medium/High)

- Operational/observability
  - API latency (95th percentile) for `/chat` and `/stream_chat`
  - Agent fallback rate (times hybrid search invoked)
  - PostgreSQL insertion success/failure rate

Suggested alert thresholds:
- JSON_parsing_failure_rate > 5% → investigate model prompt or output sanitization
- Agent fallback rate > 20% → expand KB content or refresh embeddings
- API 95p latency > 2s → scale UI/API or review model execution times

## Notes
- Keep secrets (API keys, DB passwords) in a .env file and ensure .env is in .gitignore.
- Validate model outputs programmatically (schema checks) before inserting into Postgres.
- Use provenance requirements from the agent prompt: always include KB id/url and relevance for traceability.

## MindsDB Agent Screenshots
<img width="1115" height="727" alt="Screenshot 2025-11-02 at 10 33 02 PM" src="https://github.com/user-attachments/assets/186004b3-af50-4828-a0a5-d4164b3019be" />

<img width="1115" height="568" alt="Screenshot 2025-11-02 at 10 33 08 PM" src="https://github.com/user-attachments/assets/326301ce-5dc6-4f23-8ed1-6ede32df92be" />

<img width="1115" height="568" alt="Screenshot 2025-11-02 at 10 32 33 PM" src="https://github.com/user-attachments/assets/faa7558d-4690-4391-b4f6-69148aa72dc9" /> 

<img width="1288" height="894" alt="Screenshot 2025-11-02 at 9 52 09 PM" src="https://github.com/user-attachments/assets/4e81eab9-5ca1-4722-b633-ccc705fa9334" />

## App Screenshots

<img width="1723" height="941" alt="Screenshot 2025-11-02 at 11 45 30 PM" src="https://github.com/user-attachments/assets/83823c68-e7bb-42bf-a8bb-f0b4747a96e6" />

