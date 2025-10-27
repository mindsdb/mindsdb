# AutoBanking Customer Service Workflow

**A MindsDB Use Case Demo**

An intelligent automated customer service pipeline for banking operations that showcases MindsDB's AI agent orchestration, RAG knowledge base, and enterprise integration capabilities.

✨ **[Read the Blog Post on DEV](https://dev.to/jiaqicheng1998/building-agentic-workflow-auto-banking-customer-service-with-mindsdb-484p)**

---

## 📑 Table of Contents

- [Problem Statement](#-problem-statement)
- [Architecture Overview](#-architecture-overview)
- [Knowledge Base Schema](#-knowledge-base-schema)
- [Getting Started](#-getting-started)
- [Results & Impact](#-results--impact)
- [Project Structure](#-project-structure)
- [Related Resources](#-related-resources)

---

## 🎯 Problem Statement

### The Challenge

When a customer contacts a bank for support, the manual process involves significant overhead:

**Current Manual Workflow Problems:**
- **Time-intensive**: Customer service agents must simultaneously listen, take notes, type into CRM systems (Salesforce), and classify issues
- **Context switching**: Agents juggle multiple applications and interfaces during each interaction
- **Manual escalation**: Business owners manually review unresolved cases in Salesforce and create Jira stories for tracking
- **Human fatigue**: Repetitive tasks lead to inconsistent categorization and missed escalations
- **Delayed resolution**: Manual triaging and ticket creation can take 15+ minutes per complex case

**Business Impact:**
- Reduced agent productivity due to administrative overhead
- Inconsistent case classification and prioritization
- Delayed escalation of critical issues
- Poor visibility into customer satisfaction trends
- High operational costs for routine case management

### The Solution

AutoBankingCustomerService automates the entire post-interaction workflow using MindsDB's AI agent orchestration platform:

1. **Automatic Summarization**: Every customer call is automatically summarized into concise, actionable text
2. **Intelligent Classification**: AI determines whether issues are Resolved or Unresolved based on conversation context
3. **Seamless Integration**: All conversations are automatically logged to Salesforce for CRM tracking
4. **Context-Aware Recommendations**: Unresolved cases trigger RAG-powered recommendations based on enterprise knowledge bases (Confluence)
5. **Automatic Escalation**: Unresolved issues are automatically converted into Jira tickets with recommended actions

**Key Results:**
- **84% time reduction**: Cases that required 15+ minutes of manual work now complete in under 2 minutes
- **Zero manual intervention**: Complete automation from transcript to ticket creation
- **Consistent quality**: Standardized classification and recommendation logic across all cases
- **Rapid deployment**: Built and deployed production-ready system in 48 hours

---

## 🏗️ Architecture Overview

### System Architecture Diagram
![1](./assets/arch.PNG)


### Architecture Components

#### 1. **Data Sources Layer**
- **PostgreSQL**: Stores raw call transcripts from Gong (mocked in development)
  - Table: `banking_conversations` (raw messages)
  - Table: `banking_conversations_preprocessed` (aggregated conversations)
- **Confluence**: Enterprise knowledge base with customer complaint handling policies
- **Salesforce**: CRM system for case management (write-only from our system)

#### 2. **MindsDB Layer** (AI Orchestration Hub)

**Knowledge Base (RAG Engine)**:
- Ingests Confluence pages and creates vector embeddings
- Enables semantic search over enterprise documentation
- Automatically injects relevant context into agent prompts

**Classification Agent**:
- Input: Raw conversation text from PostgreSQL
- Output: Summary (2-3 sentences) + Status (RESOLVED/UNRESOLVED)
- Model: GPT-4o or efficient equivalent

**Recommendation Agent**:
- Input: Unresolved conversation + Knowledge Base context
- Output: Actionable recommendations based on enterprise policies
- Model: GPT-4o (higher capability for policy reasoning)

#### 3. **Orchestration Layer** (Python Backend)

**FastAPI Server**: Coordinates the entire workflow
- Receives call transcripts via API
- Queries MindsDB agents
- Writes results to Salesforce and Jira
- Maintains security boundaries (all write operations go through our backend)

#### 4. **Output Systems**
- **Salesforce**: Receives ALL conversations with summaries and classification
- **Jira**: Receives ONLY unresolved cases with AI-generated recommendations

### Data Flow Sequence

```
1. Call Transcript → PostgreSQL
2. Python Backend → Query Classification Agent (MindsDB)
3. Classification Agent → Return Summary + Status
4. Python Backend → Create Salesforce Case (ALL conversations)
5. IF Status = UNRESOLVED:
   a. Python Backend → Query Recommendation Agent (MindsDB)
   b. Recommendation Agent → Query Knowledge Base (RAG)
   c. Knowledge Base → Return Relevant Policy Docs
   d. Recommendation Agent → Generate Action Plan
   e. Python Backend → Create Jira Ticket (with recommendations)
```

---

## 📚 Knowledge Base Schema

### Overview

The Knowledge Base is MindsDB's built-in RAG (Retrieval Augmented Generation) engine that eliminates the need for custom vector database infrastructure. It automatically handles:
- Document chunking
- Embedding generation (OpenAI `text-embedding-3-small`)
- Vector indexing
- Semantic search
- Context injection into agent prompts

### Schema Definition

```sql
CREATE KNOWLEDGE_BASE my_confluence_kb
USING
    embedding_model = {
        "provider": "openai",
        "model_name": "text-embedding-3-small",
        "api_key": "<your-api-key>"
    },
    content_columns = ['body_storage_value'],  -- Confluence page content
    id_column = 'id';                          -- Unique page identifier
```

### Data Ingestion

```sql
-- Ingest specific Confluence pages into the Knowledge Base
INSERT INTO my_confluence_kb (
    SELECT id, title, body_storage_value
    FROM my_confluence.pages
    WHERE id IN ('360449', '589825')  -- Customer Complaint Handling pages
);
```

### Knowledge Base Queries

```sql
-- Verify ingestion
SELECT COUNT(*) as total_chunks FROM my_confluence_kb;

-- Search for specific content
SELECT chunk_content
FROM my_confluence_kb
WHERE chunk_content ILIKE '%complaint escalation%'
LIMIT 5;

-- Inspect Knowledge Base structure
DESCRIBE KNOWLEDGE_BASE my_confluence_kb;
```

### Agent Integration

The Recommendation Agent automatically queries the Knowledge Base without explicit SELECT statements:

```sql
CREATE AGENT recommendation_agent
USING
    model = {
        "provider": "openai",
        "model_name": "gpt-4o",
        "api_key": "<your-api-key>"
    },
    data = {
        "knowledge_bases": ["mindsdb.my_confluence_kb"]  -- Auto-inject context
    },
    prompt_template = 'You are a Banking Customer Issue Resolution Consultant.
    Use my_confluence_kb to reference official policies and procedures.

    Provide clear operational recommendations for this UNRESOLVED case:
    {{question}}';
```

### Content Sources

Our Knowledge Base contains two key Confluence pages:

| Page ID | Title                                  | Purpose                                      |
|---------|----------------------------------------|----------------------------------------------|
| 360449  | Customer Complaints Management Policy  | Official complaint handling procedures       |
| 589825  | Complaint Handling Framework           | Escalation workflows and resolution criteria |


### Environment Variables

The `.env.example` file includes all configuration options:

**Required:**
- `OPENAI_API_KEY`: Your OpenAI API key for AI agents

**Pre-configured (Docker Compose):**
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`: PostgreSQL settings
- `MINDSDB_URL`: MindsDB HTTP endpoint

**Optional (for full integration):**
- `SALESFORCE_*`: Salesforce CRM credentials
- `JIRA_*`: Jira workspace credentials
- `CONFLUENCE_*`: Confluence knowledge base credentials

### Testing

```bash
# Test single conversation processing
python3.11 test_single_conversation.py

# Test recommendation workflow (includes AI recommendations)
python3.11 test_recommendation.py
```

### Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove all data
docker-compose down -v
```

---

## 🌟 MindsDB Features Demonstrated

This use case showcases several key MindsDB capabilities:

### 1. **Declarative AI Agent Creation**

Instead of building custom LLM orchestration logic, define agents with SQL:

```sql
CREATE AGENT classification_agent
USING
    data = {
        "tables": ["banking_postgres_db.conversations_summary"]
    },
    prompt_template = 'Analyze this conversation and provide summary + status...',
    timeout = 30;
```

**What MindsDB handles automatically:**
- Prompt template management
- Model API calls and retry logic
- Response parsing and validation
- Agent versioning and observability

### 2. **Zero-Infrastructure RAG**

Traditional RAG setup requires:
```python
# ❌ Without MindsDB: ~500+ lines of custom code
vector_db = Pinecone(...)
embeddings = OpenAIEmbeddings(...)
documents = load_and_chunk_docs(...)
vectors = embeddings.embed_documents(documents)
vector_db.add_vectors(vectors)
# ...plus retrieval logic, reranking, context injection, etc.
```

With MindsDB:
```sql
-- ✅ With MindsDB: 3 SQL statements
CREATE KNOWLEDGE_BASE my_confluence_kb USING embedding_model = {...};
INSERT INTO my_confluence_kb (SELECT * FROM my_confluence.pages);
CREATE AGENT recommendation_agent USING data = {"knowledge_bases": ["my_confluence_kb"]};
```

### 3. **Unified Data Access**

Query AI agents, databases, and APIs with the same SQL interface:

```sql
-- Query PostgreSQL database
SELECT * FROM banking_postgres_db.conversations_summary WHERE resolved = FALSE;

-- Query AI agent
SELECT answer FROM classification_agent WHERE question = 'conversation text';

-- Query Confluence (via MindsDB connector)
SELECT * FROM my_confluence.pages WHERE title LIKE '%complaint%';
```

### 4. **Built-in Observability**

Every agent interaction is traceable through MindsDB's UI:
- Input prompts and retrieved context
- Model reasoning steps
- Generated outputs
- Execution time and token usage

This transparency is critical for debugging and compliance in regulated industries like banking.

---

## 🛠️ Technology Stack

### Core Platform
- **MindsDB** (v24.x): AI/ML orchestration platform
  - AI Agent management
  - RAG Knowledge Base engine
  - Enterprise data connectors

### Data Sources
- **PostgreSQL** (v13): Conversation data storage
- **Confluence**: Knowledge base documentation
- **Salesforce**: CRM integration (output)
- **Jira**: Issue tracking (output)

### Application Layer
- **FastAPI**: Python web framework for API endpoints
- **Python 3.11+**: Application logic and orchestration
- **Docker**: Containerized deployment

### AI Models
- **OpenAI GPT-4o**: Classification and recommendation agents
- **OpenAI text-embedding-3-small**: Knowledge Base embeddings

---

## Results & Impact

### Operational Efficiency
- **84% time reduction**: From 15+ minutes to under 2 minutes per case
- **Zero manual intervention**: Complete end-to-end automation
- **Automated routing**: Intelligent escalation to appropriate teams

## Project Structure

```
AutoBankingCustomerService/
├── app/                          # Core application code
│   ├── __init__.py              # FastAPI app initialization
│   ├── api.py                   # API routes and schemas
│   ├── db.py                    # Database utilities
│   ├── services.py              # Business logic
│   ├── mindsdb.py               # MindsDB client
│   ├── jira_client.py           # Jira integration
│   ├── salesforce_client.py     # Salesforce integration
│   └── recommendation_client.py # AI recommendation client
├── script/                       # Data import scripts
│   ├── banking_sample_10k.csv  # Sample banking data
│   └── import_banking_data.py   # Data import utility
├── test_*.py                     # Test scripts
├── server.py                     # Application entry point
├── mindsdb_setup.sql            # MindsDB configuration
├── env.template                 # Environment variables template
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## 📝 API Endpoints

### Core Endpoints
- `GET /health`: Health check endpoint
- `POST /api/process-conversations`: Process customer conversations in batch

### Request Format
```json
{
  "conversation_texts": [
    "agent: Hello, how can I help you today?\nclient: I have an issue with my account..."
  ]
}
```

### Response Format
```json
{
  "success": true,
  "total_conversations": 1,
  "processed_count": 1,
  "processing_time_seconds": 2.5,
  "cases": [
    {
      "conversation_id": "uuid",
      "summary": "AI-generated summary",
      "status": "UNRESOLVED",
      "jira_issue_key": "BCS-123",
      "jira_issue_url": "https://...",
      "salesforce_case_id": "500...",
      "salesforce_case_url": "https://...",
      "recommendation": "AI-generated recommendations"
    }
  ]
}
```

---

## 🔗 Related Resources

- **MindsDB Documentation**: https://docs.mindsdb.com
- **MindsDB GitHub**: https://github.com/mindsdb/mindsdb
- **Community Slack**: https://mindsdb.com/joincommunity
- **More Use Cases**: https://github.com/mindsdb/mindsdb/tree/main/use-cases

---

## 📄 License

This use case demo is part of the MindsDB project and is licensed under the GNU General Public License v3.0.

---

**Built for Hacktoberfest 2025| Powered by MindsDB**

> This demo was created to showcase MindsDB's capabilities in building production-ready AI applications with minimal infrastructure. It demonstrates how enterprises can automate complex workflows by combining AI agents, RAG knowledge bases, and existing data sources through a unified SQL interface.
