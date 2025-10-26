# 🏗️ Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA LAYER                                │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Support Tickets Dataset (CSV)                            │  │
│  │  - 25 tickets with descriptions, resolutions              │  │
│  │  - Categories: Technical, Billing, Account, Features     │  │
│  │  - Metadata: Priority, Status, Dates, Agents             │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                               │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Data Preparation                                         │  │
│  │  - Combine text fields (subject + description + resolution)│ │
│  │  - Extract metadata (category, priority, status, etc.)   │  │
│  │  - Format for Knowledge Base ingestion                   │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                  MINDSDB KNOWLEDGE BASE                          │
│                                                                   │
│  ┌─────────────────────┐      ┌──────────────────────────────┐ │
│  │  Embedding Model    │      │  Vector Storage              │ │
│  │  - OpenAI           │──────▶  - ChromaDB                  │ │
│  │  - Text → Vectors   │      │  - Similarity Search         │ │
│  └─────────────────────┘      └──────────────────────────────┘ │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Metadata Index                                           │  │
│  │  - ticket_id, category, priority, status                 │  │
│  │  - created_date, customer_name, agent_name               │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                     QUERY LAYER                                  │
│                                                                   │
│  ┌────────────────────┐         ┌──────────────────────────┐   │
│  │  Semantic Search   │         │  Hybrid Search           │   │
│  │  - Natural language│         │  - Semantic + Filters    │   │
│  │  - Vector similarity│        │  - Category, Priority    │   │
│  └────────────────────┘         └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   APPLICATION LAYER                              │
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐ │
│  │  Demo Script │  │  Jupyter     │  │  Use Cases           │ │
│  │  - CLI Demo  │  │  Notebook    │  │  - Agent Assistant   │ │
│  │  - Examples  │  │  - Interactive│  │  - Issue Detection   │ │
│  └──────────────┘  └──────────────┘  └──────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   EVALUATION LAYER                               │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Metrics                                                  │  │
│  │  - Hit@1: 80%  (top result correct)                      │  │
│  │  - Hit@3: 100% (correct in top 3)                        │  │
│  │  - MRR: 0.85   (avg position 1.2)                        │  │
│  │  - Speed: <1s  (query response time)                     │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Ingestion Flow
```
CSV File → Pandas DataFrame → Text Combination → Metadata Extraction
    ↓
MindsDB KB Insert → OpenAI Embeddings → ChromaDB Storage
```

### 2. Query Flow
```
User Query (Natural Language)
    ↓
MindsDB Knowledge Base
    ↓
OpenAI Embedding (Query → Vector)
    ↓
ChromaDB Similarity Search
    ↓
Apply Metadata Filters (if hybrid search)
    ↓
Rank Results by Similarity
    ↓
Return Top K Results with Metadata
```

### 3. Evaluation Flow
```
Test Query → Search KB → Get Results → Find Expected Ticket Position
    ↓
Calculate Hit@k (is expected in top k?)
    ↓
Calculate Reciprocal Rank (1/position)
    ↓
Aggregate Metrics (Hit@1, Hit@3, MRR)
```

## Component Details

### Knowledge Base Configuration
```python
{
    "name": "support_tickets_kb",
    "model": "openai",           # Embedding model
    "storage": "chromadb",       # Vector storage
    "content_field": "content",  # Text to embed
    "metadata_fields": [         # Filterable fields
        "ticket_id",
        "category",
        "priority",
        "status",
        "created_date",
        "customer_name"
    ]
}
```

### Search Types

#### Semantic Search
```python
# Pure vector similarity
results = kb.search(
    query="Users cannot login",
    limit=5
)
# Returns: Most semantically similar tickets
```

#### Hybrid Search
```python
# Vector similarity + metadata filters
results = kb.search(
    query="database problems",
    limit=5,
    filters={
        "priority": "Critical",
        "category": "Technical"
    }
)
# Returns: Similar tickets matching filters
```

## Technology Stack

### Core Technologies
- **MindsDB**: Knowledge Base platform
- **OpenAI**: Embedding model (text-embedding-ada-002)
- **ChromaDB**: Vector database storage
- **Python 3.8+**: Application language

### Libraries
- **mindsdb-sdk**: MindsDB Python SDK
- **pandas**: Data manipulation
- **jupyter**: Interactive notebooks
- **matplotlib/seaborn**: Visualization
- **python-dotenv**: Configuration

### Infrastructure
- **MindsDB Cloud**: Hosted platform (or local)
- **GitHub**: Code repository
- **YouTube/Loom**: Demo video hosting

## Scalability Considerations

### Current Setup (Demo)
- **Data**: 25 tickets
- **Storage**: ChromaDB (in-memory)
- **Queries**: Single-user, synchronous
- **Performance**: <1s per query

### Production Scaling
- **Data**: 10,000+ tickets
- **Storage**: Persistent ChromaDB or Pinecone
- **Queries**: Multi-user, async
- **Caching**: Redis for frequent queries
- **Updates**: Scheduled jobs for new tickets
- **Monitoring**: Query performance tracking

## Security & Privacy

### Data Protection
- Environment variables for credentials (.env)
- No hardcoded API keys
- .gitignore for sensitive files

### Access Control
- MindsDB authentication required
- Knowledge Base permissions
- API key rotation (production)

### Data Privacy
- Sample data only (no real customer info)
- Anonymize PII in production
- Comply with data regulations

## Extension Points

### 1. Additional Data Sources
```python
# Connect to PostgreSQL
server.databases.create(
    name='postgres_db',
    engine='postgres',
    connection_args={...}
)

# Ingest from database
INSERT INTO support_tickets_kb
SELECT * FROM postgres_db.tickets
```

### 2. Agent Integration
```python
# Create conversational agent
agent = server.agents.create(
    name='support_agent',
    model='gpt-4',
    knowledge_bases=['support_tickets_kb']
)

# Chat with agent
response = agent.completion(
    "What are common login issues?"
)
```

### 3. Automated Updates
```sql
-- Schedule periodic updates
CREATE JOB support_kb_updater (
    INSERT INTO support_tickets_kb
    SELECT * FROM new_tickets
    WHERE created_date > LAST_RUN_TIME
)
EVERY 1 hour;
```

### 4. Custom Embeddings
```python
# Use custom embedding model
kb = server.knowledge_bases.create(
    name='support_kb_custom',
    model='huggingface/sentence-transformers',
    model_params={
        'model_name': 'all-MiniLM-L6-v2'
    }
)
```

## Performance Optimization

### Query Optimization
1. **Limit results**: Only fetch what you need
2. **Cache frequent queries**: Use Redis/memory cache
3. **Batch queries**: Group similar searches
4. **Index metadata**: Ensure filters are indexed

### Ingestion Optimization
1. **Batch inserts**: Insert multiple records at once
2. **Async processing**: Use background jobs
3. **Incremental updates**: Only add new/changed tickets
4. **Deduplication**: Avoid duplicate entries

### Storage Optimization
1. **Compression**: Compress vector data
2. **Pruning**: Remove old/irrelevant data
3. **Sharding**: Distribute across multiple stores
4. **Backup**: Regular backups of KB

## Monitoring & Metrics

### Query Metrics
- Average query time
- Queries per second
- Cache hit rate
- Error rate

### Quality Metrics
- Hit@k scores
- Mean Reciprocal Rank
- User satisfaction ratings
- Resolution time improvement

### System Metrics
- KB size (number of documents)
- Storage usage
- API rate limits
- Uptime/availability

---

## Diagram Legend

```
┌─────┐
│ Box │  = Component/Layer
└─────┘

  ↓     = Data flow direction

──────  = Connection/Relationship
```

## References

- [MindsDB Architecture](https://docs.mindsdb.com/architecture)
- [Knowledge Bases Design](https://docs.mindsdb.com/knowledge-bases)
- [Vector Search Concepts](https://www.pinecone.io/learn/vector-search/)
- [RAG Architecture](https://www.anthropic.com/research/retrieval-augmented-generation)
