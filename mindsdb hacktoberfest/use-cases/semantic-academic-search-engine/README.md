# Semantica

**Connecting ideas across the frontiers of knowledge**

Semantica is an intelligent academic search and chat platform that enables researchers to explore scientific papers, patents, and preprints across multiple sources (arXiv, bioRxiv, medRxiv, chemRxiv, and patents) using advanced semantic search powered by MindsDB and PostgreSQL with pgvector.

![Made for MindsDB Hacktoberfest](https://img.shields.io/badge/Made%20for-MindsDB%20Hacktoberfest-blue)
![Python](https://img.shields.io/badge/Python-3.12+-green)
![React](https://img.shields.io/badge/React-19.2-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.120+-teal)

---

## 🎯 Features

### 🔍 Advanced Search
- **Semantic Search**: Find papers by meaning, not just keywords
- **Hybrid Search**: Combine vector similarity with traditional keyword search (configurable alpha)
- **Multi-Source**: Search across arXiv, bioRxiv, medRxiv, chemRxiv, and patent databases
- **Advanced Filters**: Filter by publication year, categories, and source
- **Relevance Ranking**: Papers ranked by semantic similarity to your query

### 💬 AI-Powered Chat
- **Multi-Document Chat**: Select up to 4 papers and chat with them simultaneously
- **MindsDB AI Agents**: Each chat session creates a dedicated AI agent with access to paper knowledge bases
- **PDF Integration**: View papers in-app while chatting
- **Context-Aware Responses**: AI understands the content of selected papers and provides accurate answers
- **Citation Support**: Get information with proper context from the papers

### 🎨 User Experience
- Clean, modern interface built with React and Tailwind CSS
- Responsive design for desktop and mobile
- Real-time search results
- Interactive PDF viewer with Google Docs integration
- Split-pane chat interface with resizable panels

---

Demo Video - [Semantica](https://youtu.be/8wIhcXm_7nk)
Blog post - 
Twitter Share - 

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       Frontend (React)                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Search Page  │  │  Chat Page   │  │  Components  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP/REST API
┌──────────────────────────▼──────────────────────────────────┐
│                    Backend (FastAPI)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Search API   │  │  Chat API    │  │ Health Check │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└──────────────────────────┬──────────────────────────────────┘
                           │ MindsDB SDK
┌──────────────────────────▼──────────────────────────────────┐
│                      MindsDB Cloud/Local                     │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Knowledge Bases (pgvector)             │    │
│  │  • Main KB: All papers with metadata & embeddings   │    │
│  │  • Dynamic KBs: Created per paper for chat          │    │
│  ├─────────────────────────────────────────────────────┤    │
│  │              AI Agents (OpenAI GPT-4o)              │    │
│  │  • Generated per chat session                        │    │
│  │  • Access to selected papers' knowledge bases        │    │
│  └─────────────────────────────────────────────────────┘    │
└──────────────────────────┬──────────────────────────────────┘
                           │ PostgreSQL Protocol
┌──────────────────────────▼──────────────────────────────────┐
│              PostgreSQL + pgvector Extension                 │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Table: paper_raw                                    │    │
│  │  • article_id, title, abstract, authors              │    │
│  │  • categories, published_year, source, pdf_url       │    │
│  │  • text (full content)                               │    │
│  │  • Vector embeddings (via pgvector)                  │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Technology Stack

**Frontend:**
- React 19.2 with TypeScript
- Vite for build tooling
- Axios for API communication
- React Markdown for rendering AI responses
- Tailwind CSS for styling

**Backend:**
- FastAPI 0.120+ (Python 3.12+)
- MindsDB SDK for AI and knowledge base management
- psycopg2 for direct PostgreSQL access
- python-dotenv for configuration
- uvicorn as ASGI server

**Data & AI:**
- PostgreSQL with pgvector extension for vector storage
- MindsDB for knowledge base management and AI agents
- OpenAI text-embedding-3-small for embeddings
- OpenAI GPT-4o for chat and reranking

---

## 📋 Prerequisites

### Required Software
- **Python 3.12+** - Backend runtime
- **Node.js 18+** - Frontend development
- **PostgreSQL 14+** with **pgvector extension** - Vector database
- **MindsDB** - Cloud account or local instance
- **OpenAI API Key** - For embeddings and chat

### System Requirements
- 4GB+ RAM
- 2GB+ free disk space
- Internet connection for MindsDB Cloud

---

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd Semantica
```

### 2. Setup PostgreSQL with pgvector

```bash
# Install PostgreSQL (if not already installed)
# Ubuntu/Debian:
sudo apt-get install postgresql postgresql-contrib

# macOS:
brew install postgresql

# Install pgvector extension
# Follow instructions at: https://github.com/pgvector/pgvector
```

**Create database and enable pgvector:**
```sql
CREATE DATABASE mydb;
\c mydb
CREATE EXTENSION vector;
```

**Create a PostgreSQL user:**
```sql
CREATE USER psql WITH PASSWORD 'psql';
GRANT ALL PRIVILEGES ON DATABASE mydb TO psql;
```

### 3. Setup MindsDB

Refer [MindsDB Installation Docs](https://docs.mindsdb.com/setup/self-hosted/docker)

### 4. Backend Setup

```bash
cd backend

# Install UV package manager (if not installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Create .env file
cp env.example .env

# Edit .env with your credentials
nano .env
```

**Required environment variables in `.env`:**
```env
# MindsDB Configuration
MINDSDB_URL=http://127.0.0.1:47334

# OpenAI API Key
OPENAI_API_KEY=sk-...

# PostgreSQL Configuration (optional, if different from config.yaml)
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_DB=mydb
POSTGRES_USER=psql
POSTGRES_PASSWORD=psql
```

**Update `config/config.yaml` if needed:**
```yaml
pgvector:
  parameters:
    host: "127.0.0.1"
    port: 5432
    database: "mydb"
    user: "psql"
    password: "psql"

knowledge_base:
  embedding_model:
    api_key: ""  # Set via OPENAI_API_KEY env var
  reranking_model:
    api_key: ""  # Set via OPENAI_API_KEY env var
```

**Run the backend:**
```bash
# The startup script will automatically:
# - Create PostgreSQL database connection in MindsDB
# - Create the paper_raw table
# - Load sample data
# - Create knowledge base with embeddings
# - Index the knowledge base

uv run uvicorn main:app --reload
```

The backend will be available at `http://localhost:8000`

**API Documentation:**
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### 5. Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Create .env file
echo "VITE_API_BASE_URL=http://localhost:8000/api/v1" > .env

# Start development server
npm run dev
```

The frontend will be available at `http://localhost:5173`

---

## 📖 Usage Guide

### Searching for Papers

1. **Enter a search query**: Type your research question or topic
2. **Apply filters** (optional):
   - Toggle hybrid search and adjust alpha (0.0 = keyword, 1.0 = semantic)
   - Select source corpora (arXiv, bioRxiv, medRxiv, chemRxiv, patents)
   - Filter by publication year or category
3. **Browse results**: Papers are ranked by relevance
4. **Select papers**: Choose up to 4 papers for chat (click bookmark icon)

### Chatting with Papers

1. **Select papers**: Choose 1-4 papers from search results
2. **Click "Chat"**: Initiates a chat session with selected papers
3. **View PDFs**: Click on papers in the left panel to view them
4. **Ask questions**: Type questions about the papers in the chat
5. **Get AI responses**: The AI agent reads the papers and provides contextual answers

### Example Queries

**Search:**
- "Machine Learning for Optics"
- "CRISPR applications in gene therapy"
- "Quantum error correction techniques"
- "Climate change prediction models"

**Chat:**
- "What are the main findings of this paper?"
- "Compare the methodologies used in these papers"
- "What are the limitations discussed?"
- "How do these papers relate to each other?"

---

## 🔧 Configuration

### Backend Configuration (`backend/config/config.yaml`)

```yaml
app:
  name: "REST API with MindsDB"
  version: "1.0.0"
  host: "0.0.0.0"
  port: 8000
  debug: true
  run_startup: true  # Enable automatic setup on startup

mindsdb:
  url: "https://cloud.mindsdb.com"  # or http://127.0.0.1:47334
  email: ""  # Set in .env
  password: ""  # Set in .env

pgvector:
  database_name: "my_pgvector"
  parameters:
    host: "127.0.0.1"
    port: 5432
    database: "mydb"
    user: "psql"
    password: "psql"

knowledge_base:
  name: "kv_kb"
  embedding_model:
    provider: "openai"
    model_name: "text-embedding-3-small"
  reranking_model:
    provider: "openai"
    model_name: "gpt-4o"

agent:
  model: "gpt-4o"
```

### Frontend Configuration (`.env`)

```env
VITE_API_BASE_URL=http://localhost:8000/api/v1
```

---

## 🗂️ Project Structure

```
Semantica/
├── README.md                   # This file
├── backend/                    # FastAPI backend
│   ├── api/                    # API routes and models
│   │   ├── __init__.py
│   │   ├── models.py           # Pydantic models
│   │   └── routes.py           # API endpoints
│   ├── config/                 # Configuration management
│   │   ├── __init__.py
│   │   ├── config.yaml         # Main config file
│   │   └── loader.py           # Config loader
│   ├── data/                   # Sample data
│   │   └── sample_data.json    # Sample papers
│   ├── mindsdb/                # MindsDB client
│   │   ├── __init__.py
│   │   └── client.py           # MindsDB wrapper
│   ├── main.py                 # FastAPI app entry point
│   ├── startup.py              # Startup operations (DB setup, KB creation)
│   ├── utils.py                # Utility functions
│   ├── pyproject.toml          # Python dependencies
│   └── README.md               # Backend documentation
└── frontend/                   # React frontend
    ├── components/             # React components
    │   ├── icons/              # Icon components
    │   ├── ChatWindow.tsx      # Chat interface
    │   ├── Header.tsx          # App header
    │   ├── PaperCard.tsx       # Paper display card
    │   ├── ResultsGrid.tsx     # Search results grid
    │   ├── SearchBar.tsx       # Search input and filters
    │   └── SelectedPapersList.tsx  # Selected papers panel
    ├── pages/                  # Page components
    │   ├── ChatPage.tsx        # Chat page
    │   └── SearchPage.tsx      # Search page
    ├── App.tsx                 # Main app component
    ├── index.tsx               # App entry point
    ├── types.ts                # TypeScript types
    ├── constants.ts            # Constants
    ├── package.json            # Node dependencies
    ├── vite.config.ts          # Vite configuration
    └── README.md               # Frontend documentation
```

---

## 🔌 API Endpoints

### Health Check
```http
GET /api/v1/health
```

### Search Papers
```http
POST /api/v1/search
Content-Type: application/json

{
  "query": "machine learning",
  "filters": {
    "isHybridSearch": true,
    "alpha": 0.7,
    "corpus": {
      "arxiv": true,
      "patent": false
    },
    "publishedYear": "2024",
    "category": "cs.LG"
  }
}
```

### Initiate Chat
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

### Send Chat Message
```http
POST /api/v1/chat/completion
Content-Type: application/json

{
  "query": "What are the main findings?",
  "agentId": "agent_abc123"
}
```

---

## 🧪 Development

### Running Tests

```bash
# Backend tests
cd backend
uv run pytest

# Frontend tests
cd frontend
npm test
```

### Code Formatting

```bash
# Backend (Python)
cd backend
uv run black .
uv run ruff check .

# Frontend (TypeScript)
cd frontend
npm run lint
```

### Building for Production

```bash
# Frontend
cd frontend
npm run build

# Serve with backend
cd backend
# Update CORS settings in main.py for production domain
uv run uvicorn main:app --host 0.0.0.0 --port 8000
```

---

## 🤝 Contributing

Contributions are welcome! This project was created for MindsDB Hacktoberfest.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is open source and available under the MIT License.

---

## 🙏 Acknowledgments

- **MindsDB** - For the amazing AI and knowledge base platform
- **OpenAI** - For GPT-4o and embedding models
- **PostgreSQL & pgvector** - For efficient vector storage and search
- **FastAPI** - For the excellent Python web framework
- **React** - For the powerful UI library
- **goose** - For building the frontend of the app

---

## 📧 Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Check MindsDB documentation: [docs.mindsdb.com](https://docs.mindsdb.com/)
- Join MindsDB community: [Slack](https://mindsdb.com/slack)

---

**Made with ❤️ for MindsDB Hacktoberfest**
