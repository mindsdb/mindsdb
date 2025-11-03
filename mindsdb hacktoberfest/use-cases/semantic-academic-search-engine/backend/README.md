# Semantica Backend

FastAPI-based backend for the Semantica academic search and chat platform, powered by MindsDB and PostgreSQL with pgvector.

## 🎯 Overview

The Semantica backend provides REST API endpoints for semantic search across academic papers and AI-powered chat functionality. It leverages MindsDB for knowledge base management and AI agents, PostgreSQL with pgvector for vector storage, and OpenAI models for embeddings and chat.

## 🛠️ Tech Stack

- **FastAPI 0.120+** - Modern web framework for building APIs
- **MindsDB SDK 3.4.8+** - AI and knowledge base management
- **PostgreSQL + pgvector** - Vector database for embeddings
- **OpenAI** - text-embedding-3-small for embeddings, GPT-4o for chat
- **Python 3.12+** - Programming language
- **UV** - Fast Python package manager
- **uvicorn** - ASGI server

## 📂 Project Structure

```
backend/
├── api/                    # API routes and models
│   ├── __init__.py        # API router export
│   ├── models.py          # Pydantic request/response models
│   └── routes.py          # API endpoint implementations
├── config/                # Configuration management
│   ├── __init__.py       # Config loader exports
│   ├── config.yaml       # Main configuration file
│   └── loader.py         # YAML configuration loader
├── data/                 # Data files
│   └── sample_data.json  # Sample academic papers
├── mindsdb/              # MindsDB client wrapper
│   ├── __init__.py      # Client singleton export
│   └── client.py        # MindsDB SDK wrapper
├── main.py              # FastAPI application entry point
├── startup.py           # Database and knowledge base setup
├── utils.py             # Utility functions (query builders, etc.)
├── pyproject.toml       # Python dependencies (UV)
├── env.example          # Environment variables template
└── README.md           # This file
```

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- UV package manager ([install instructions](https://github.com/astral-sh/uv))
- PostgreSQL 14+ with pgvector extension
- MindsDB Cloud account or local instance
- OpenAI API key

### Installation

```bash
cd backend

# Install UV (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Create environment file
cp env.example .env

# Edit .env with your credentials
nano .env
```

### Environment Configuration

Create a `.env` file with the following variables:

```env
# MindsDB Configuration
MINDSDB_URL=https://cloud.mindsdb.com

# OpenAI API Key
OPENAI_API_KEY=sk-...

# PostgreSQL (optional - defaults from config.yaml are used)
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_DB=mydb
POSTGRES_USER=psql
POSTGRES_PASSWORD=psql

# Application Settings (optional)
APP_HOST=0.0.0.0
APP_PORT=8000
```

### Run the Application

```bash
uv run python main.py
```

The backend will:
1. Load configuration from `config/config.yaml` and `.env`
2. Connect to MindsDB
3. Run startup operations:
   - Create PostgreSQL database connection in MindsDB
   - Create `paper_raw` table in PostgreSQL
   - Load sample data from `data/sample_data.json`
   - Create main knowledge base with embeddings
   - Insert data into knowledge base
   - Create knowledge base index

The API will be available at `http://localhost:8000`

## 📚 API Documentation

### Interactive Documentation

Once the server is running:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI Schema**: http://localhost:8000/openapi.json

### Endpoints

#### Health Check
```http
GET /api/v1/health
```

**Response:**
```json
{
  "status": "healthy",
  "connected": true,
  "message": "MindsDB client is ready"
}
```

#### Search Papers
```http
POST /api/v1/search
Content-Type: application/json

{
  "query": "machine learning for optics",
  "filters": {
    "isHybridSearch": true,
    "alpha": 0.7,
    "corpus": {
      "arxiv": true,
      "patent": true,
      "biorxiv": false,
      "medrxiv": false,
      "chemrxiv": false
    },
    "publishedYear": "2024",
    "category": "cs.LG"
  }
}
```

**Parameters:**
- `query` (required): Search query string
- `filters` (optional): Filter object
  - `isHybridSearch`: Enable hybrid search (semantic + keyword)
  - `alpha`: Hybrid search weight (0.0 = keyword, 1.0 = semantic)
  - `corpus`: Source selection (arxiv, patent, biorxiv, medrxiv, chemrxiv)
  - `publishedYear`: Filter by publication year
  - `category`: Filter by category (e.g., "cs.LG")

**Response:**
```json
[
  {
    "id": "CS-ML-2023-001",
    "title": "Deep Learning Approaches...",
    "authors": "Smith, J.; Johnson, M.",
    "date": "2023",
    "abstract": "This paper presents...",
    "categories": ["Computer Science", "Machine Learning"],
    "url": "https://arxiv.org/pdf/...",
    "source": "arxiv"
  }
]
```

#### Initiate Chat Session
```http
POST /api/v1/chat/initiate
Content-Type: application/json

{
  "papers": [
    {"id": "CS-ML-2023-001", "source": "arxiv"},
    {"id": "PHY-QC-2024-042", "source": "patent"}
  ]
}
```

**Process:**
1. Creates individual knowledge bases for each selected paper
2. Populates knowledge bases with paper content
3. Creates an AI agent with access to all paper knowledge bases
4. Returns agent ID and document information

**Response:**
```json
{
  "aiAgentId": "agent_abc123xyz",
  "documents": [
    {
      "paperId": "CS-ML-2023-001",
      "paperUrl": "https://arxiv.org/pdf/...",
      "title": "Deep Learning Approaches...",
      "source": "arxiv"
    }
  ]
}
```

#### Send Chat Message
```http
POST /api/v1/chat/completion
Content-Type: application/json

{
  "query": "What are the main findings of these papers?",
  "agentId": "agent_abc123xyz"
}
```

**Response:**
```json
{
  "answer": "Based on the papers provided, the main findings are..."
}
```

## ⚙️ Configuration

### config.yaml

Main configuration file located at `config/config.yaml`:

```yaml
app:
  name: "REST API with MindsDB"
  version: "1.0.0"
  host: "0.0.0.0"
  port: 8000
  debug: true
  run_startup: true  # Enable/disable automatic setup

mindsdb:
  url: "https://cloud.mindsdb.com"  # or http://127.0.0.1:47334
  email: ""  # Set in .env
  password: ""  # Set in .env

pgvector:
  database_name: "my_pgvector"
  engine: "pgvector"
  parameters:
    host: "127.0.0.1"
    port: 5432
    database: "mydb"
    user: "psql"
    password: "psql"
    distance: "cosine"

knowledge_base:
  name: "kv_kb"
  storage_table: "pgvector_storage_table"
  embedding_model:
    provider: "openai"
    model_name: "text-embedding-3-small"
    api_key: ""  # Set in .env as OPENAI_API_KEY
  reranking_model:
    provider: "openai"
    model_name: "gpt-4o"
    api_key: ""  # Set in .env as OPENAI_API_KEY
  metadata_columns:
    - "authors"
    - "categories"
    - "published_year"
    - "article_id"
    - "title"
    - "abstract"
    - "source"
    - "pdf_url"
  content_columns:
    - "text"

agent:
  model: "gpt-4o"
```

### Environment Variables

Environment variables override config.yaml values:

| Variable | Description | Default |
|----------|-------------|---------|
| `MINDSDB_EMAIL` | MindsDB account email | - |
| `MINDSDB_PASSWORD` | MindsDB account password | - |
| `MINDSDB_URL` | MindsDB server URL | `https://cloud.mindsdb.com` |
| `OPENAI_API_KEY` | OpenAI API key | - |
| `POSTGRES_HOST` | PostgreSQL host | `127.0.0.1` |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `POSTGRES_DB` | PostgreSQL database | `mydb` |
| `POSTGRES_USER` | PostgreSQL user | `psql` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `psql` |
| `APP_HOST` | API server host | `0.0.0.0` |
| `APP_PORT` | API server port | `8000` |

## 🗄️ Database Schema

### PostgreSQL Table: `paper_raw`

```sql
CREATE TABLE paper_raw (
    id SERIAL PRIMARY KEY,
    article_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    abstract VARCHAR NOT NULL,
    authors VARCHAR NOT NULL,
    categories VARCHAR NOT NULL,
    published_year VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    pdf_url VARCHAR NOT NULL,
    text VARCHAR NOT NULL
);
```

**Columns:**
- `id`: Auto-incrementing primary key
- `article_id`: Unique paper identifier
- `title`: Paper title
- `abstract`: Paper abstract
- `authors`: Comma-separated author list
- `categories`: Comma-separated categories
- `published_year`: Publication year
- `source`: Source corpus (arxiv, patent, etc.)
- `pdf_url`: URL to PDF
- `text`: Full paper content

## 🧠 MindsDB Knowledge Bases

### Main Knowledge Base (`kv_kb`)

Created from `paper_raw` table with:
- **Embeddings**: Generated using OpenAI text-embedding-3-small
- **Metadata**: All metadata columns for filtering
- **Content**: Full text for semantic search
- **Index**: Created for fast similarity search

### Dynamic Knowledge Bases

For each chat session, individual knowledge bases are created per paper:
- Name format: `{article_id}_kb`
- Contains single paper's content
- Used by AI agent for context-aware responses

## 🤖 AI Agents

AI agents are created dynamically for chat sessions:

- **Model**: GPT-4o
- **Knowledge Bases**: Access to selected papers' knowledge bases
- **Custom Prompt**: Instructions for answering questions about papers
- **Name**: Randomly generated (e.g., `agent_abc123xyz`)

## 🔧 Core Modules

### MindsDB Client (`mindsdb/client.py`)

Wrapper around MindsDB SDK:

```python
from mindsdb import get_mindsdb_client

client = get_mindsdb_client()
client.connect()  # Connect using config credentials

# Execute queries
result = client.query("SELECT * FROM mindsdb.models")
data = result.fetch()

client.is_connected()  # Check connection status
client.disconnect()  # Close connection
```

### Configuration Loader (`config/loader.py`)

Loads and merges configuration from YAML and environment variables:

```python
from config import load_config, get_config

load_config()  # Load config once
config = get_config()  # Get config instance

# Access nested values with dot notation
host = config.get("app.host", "0.0.0.0")
kb_name = config.get("knowledge_base.name")
```

### Startup Operations (`startup.py`)

Automated setup functions:

- `setup_pgvector_database()`: Create PostgreSQL connection in MindsDB
- `create_postgres_table()`: Create paper_raw table
- `insert_data_to_postgres(data)`: Insert papers into table
- `setup_knowledge_base()`: Create knowledge base in MindsDB
- `insert_to_knowledge_base()`: Populate knowledge base from PostgreSQL
- `create_knowledge_base_index()`: Index knowledge base for search
- `load_sample_data()`: Load and insert sample data from JSON
- `run_startup_operations()`: Run all startup operations

### Utility Functions (`utils.py`)

Helper functions for query building and data transformation:

- `build_query_for_kb()`: Build knowledge base CREATE/INSERT queries
- `build_create_agent_query()`: Build AI agent CREATE query
- `build_custom_prompt_template()`: Generate agent prompt
- `build_psql_select_query()`: Build PostgreSQL SELECT query
- `transform_results()`: Transform search results for frontend
- `generate_random_agent_name()`: Generate unique agent names
- `replace_punctuation_with_underscore()`: Sanitize names for MindsDB

## 🔄 Application Lifecycle

### Startup

1. Load configuration from `config.yaml` and `.env`
2. Connect to MindsDB
3. Run startup operations (if enabled):
   - Setup database connections
   - Create tables
   - Load sample data
   - Create knowledge bases
4. Register API routes
5. Start uvicorn server

### Runtime

1. Accept API requests
2. Validate request data (Pydantic models)
3. Execute MindsDB queries
4. Transform responses
5. Return JSON responses

### Shutdown

1. Disconnect from MindsDB
2. Close database connections
3. Cleanup resources

## 🧪 Development

### Running in Development Mode

```bash
# With auto-reload
uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Or using main.py (uses config settings)
uv run python main.py
```

### Code Formatting

```bash
# Install dev tools
uv add --dev black ruff pytest

# Format code
uv run black .

# Lint code
uv run ruff check .

# Fix linting issues
uv run ruff check --fix .
```

### Testing

```bash
# Install test dependencies
uv add --dev pytest httpx

# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=.
```

## 📊 Logging

The application uses Python's built-in logging:

```python
import logging
logger = logging.getLogger(__name__)

logger.info("Informational message")
logger.warning("Warning message")
logger.error("Error message")
```

Logs include:
- Startup operations progress
- API requests and responses
- MindsDB queries
- Error tracebacks

## 🐛 Troubleshooting

### MindsDB Connection Issues

**Problem**: Cannot connect to MindsDB

**Solutions**:
- Verify credentials in `.env`
- Check MindsDB URL (cloud vs local)
- Ensure MindsDB Cloud account is active
- For local: Ensure MindsDB is running on port 47334

### PostgreSQL Connection Issues

**Problem**: Cannot connect to PostgreSQL

**Solutions**:
- Verify PostgreSQL is running: `pg_isready`
- Check credentials in config.yaml
- Ensure database exists: `psql -l`
- Verify pgvector extension: `psql -c "SELECT * FROM pg_extension WHERE extname='vector'"`

### Knowledge Base Creation Fails

**Problem**: Knowledge base creation returns error

**Solutions**:
- Ensure OpenAI API key is valid
- Check that PostgreSQL connection exists in MindsDB
- Verify table `paper_raw` has data
- Check MindsDB logs for detailed error

### Startup Operations Skip

**Problem**: Startup operations not running

**Solutions**:
- Check `app.run_startup` in config.yaml is `true`
- Review application logs for errors
- Manually run: `uv run python startup.py`

## 🚀 Production Deployment

### Using Docker

Create a `Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install UV
RUN pip install uv

# Copy project files
COPY pyproject.toml .
COPY . .

# Install dependencies
RUN uv sync

# Expose port
EXPOSE 8000

# Run application
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Build and run:

```bash
docker build -t semantica-backend .
docker run -p 8000:8000 --env-file .env semantica-backend
```

### Environment Variables for Production

Update production `.env`:

```env
MINDSDB_URL=https://cloud.mindsdb.com
APP_DEBUG=false
APP_RUN_STARTUP=false  # Run startup manually in production
```

### CORS Configuration

Update `main.py` for production domains:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://your-frontend-domain.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

## 📚 Additional Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [MindsDB Documentation](https://docs.mindsdb.com/)
- [PostgreSQL pgvector](https://github.com/pgvector/pgvector)
- [UV Package Manager](https://github.com/astral-sh/uv)
- [Pydantic Documentation](https://docs.pydantic.dev/)

---

**Made with ❤️ for MindsDB Hacktoberfest**
