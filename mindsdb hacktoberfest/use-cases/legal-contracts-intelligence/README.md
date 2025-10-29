# Legal Contract Analyzer

> AI-powered legal contract analysis platform using MindsDB Knowledge Bases for intelligent document processing, risk assessment, and automated reporting.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Node.js](https://img.shields.io/badge/node-%3E%3D18.17.0-brightgreen)
![Python](https://img.shields.io/badge/python-%3E%3D3.8-blue)
![MindsDB](https://img.shields.io/badge/MindsDB-v24.x-orange)
![Docker](https://img.shields.io/badge/docker-ready-blue)

https://github.com/user-attachments/assets/b4ffa79e-f270-41a7-9309-f956062c6e33

## ⚡ Quick Deploy (2 minutes)

**Try it now with one command - no setup required!**

```bash
# Create .env file with your OpenAI or Gemini API key, then run:
curl -o docker-compose.yml https://raw.githubusercontent.com/ritwickrajmakhal/legal-contract-analyzer/main/docker-compose.prod.yml && docker-compose up -d
```

**Access your AI contract analyzer at:** http://localhost:3000

### What You Get Instantly

✅ **Complete AI Contract Analysis Platform**  
✅ **Pre-configured MindsDB Knowledge Base**  
✅ **Multi-source Data Integration (PostgreSQL, SharePoint, Dropbox, etc.)**  
✅ **Interactive Chat Interface with Natural Language Queries**  
✅ **Risk Assessment & Compliance Monitoring**  
✅ **Automated Email Notifications & Reporting**

## 🚀 Key Features

- **🧠 AI-Powered Analysis**: OpenAI GPT-4 & Google Gemini contract analysis
- **📊 Smart Knowledge Base**: MindsDB vector search across contract portfolios
- **⚡ Natural Language Chat**: Ask questions about contracts in plain English
- **📈 Risk Assessment**: Automated risk scoring and compliance checking
- **📧 Email Automation**: Smart notifications and reporting
- **🔗 Multi-Source Integration**: PostgreSQL, SharePoint, Dropbox, Salesforce, GitHub, and more
- **� PDF Processing**: Drag-and-drop contract uploads with automatic parsing
- **📅 Deadline Tracking**: Renewal reminders and critical date monitoring

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Frontend Layer (Next.js - Port 3000)               │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  Chat Interface │  │    Analytics    │  │  Integrations   │              │
│  │  • ChatContainer│  │  • Risk Gauges  │  │  • Data Sources │              │
│  │  • Composer     │  │  • Timeline     │  │  • KB Manager   │              │
│  │  • Message      │  │  • Charts       │  │  • Sync Status  │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ HTTP/REST API
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Backend Layer (FastAPI - Port 8000)                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  Agent Routes   │  │ Integration API │  │   Upload API    │              │
│  │  • Chat API     │  │ • Data Sources  │  │ • PDF Upload    │              │
│  │  • Streaming    │  │ • Connection    │  │ • File Parsing  │              │
│  │  • Analytics    │  │ • Table Sync    │  │ • Validation    │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
│                                │                                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │   KB Manager    │  │ Email Actions   │  │ Core Services   │              │
│  │ • KB Operations │  │ • SMTP Service  │  │ • MindsDB Mgr   │              │
│  │ • Search API    │  │ • Notifications │  │ • Query Gen     │              │
│  │ • Sync Jobs     │  │ • Reporting     │  │ • Health Check  │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ MindsDB SDK
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       AI/Database Layer (MindsDB - Port 47334)              │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │ Knowledge Base  │  │   AI Models     │  │   Integrations  │              │
│  │ • Vector Store  │  │ • OpenAI GPT-4  │  │ • PostgreSQL    │              │
│  │ • Document KB   │  │ • Google Gemini │  │ • SharePoint    │              │
│  │ • Search Index  │  │ • Embeddings    │  │ • Dropbox       │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
│                                │                                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │     Agents      │  │    Projects     │  │   Data Flows    │              │
│  │ • Contract AI   │  │ • Workspaces    │  │ • ETL Pipeline  │              │
│  │ • Risk Analysis │  │ • Permissions   │  │ • Auto Sync     │              │
│  │ • Q&A System    │  │ • Configs       │  │ • Notifications │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Data Sources Layer                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │   Databases     │  │  Cloud Storage  │  │   Enterprise    │              │
│  │ • PostgreSQL    │  │ • SharePoint    │  │ • Salesforce    │              │
│  │ • Snowflake     │  │ • Dropbox       │  │ • Elasticsearch │              │
│  │ • Files/PDF     │  │ • GitHub        │  │ • Apache Solr   │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
│                                │                                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │ Communication   │  │   Productivity  │  │   Development   │              │
│  │ • Email/SMTP    │  │ • Notion        │  │ • GitLab        │              │
│  │ • Notifications │  │ • Workspaces    │  │ • Repositories  │              │
│  │ • Reporting     │  │ • Knowledge     │  │ • CI/CD         │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘

                              Data Flow & Security
┌─────────────────────────────────────────────────────────────────────────────┐
│ • PDF Upload → FastAPI → MindsDB Knowledge Base → Vector Embeddings         │
│ • User Query → Chat API → AI Agent → Knowledge Base Search → LLM Response   │
│ • Data Sync → Integration API → MindsDB → Automated Knowledge Base Update   │
│ • Risk Analysis → Agent API → ML Models → Interactive Analytics Dashboard   │
│ • Email Actions → SMTP Service → Automated Notifications & Reports          │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📋 Prerequisites

- **Node.js** >= 18.17.0
- **Python** >= 3.8
- **Docker** and **Docker Compose** (recommended)
- **API Keys**: OpenAI or Google Gemini API key for AI analysis

## 🚀 Quick Start

### Docker Deployment (Recommended)

**Deploy instantly with one command:**

1. **Create your `.env` file**
   ```bash
   # Required: AI Model API Key (choose one)
   OPENAI_API_KEY=your-openai-api-key-here
   # OR
   GEMINI_API_KEY=your-gemini-api-key-here

   # Optional: Email notifications
   SMTP_USER=your-email@gmail.com
   SMTP_PASSWORD=your-app-password
   ```

2. **Run the deployment command:**
   ```bash
   curl -o docker-compose.yml https://raw.githubusercontent.com/ritwickrajmakhal/legal-contract-analyzer/main/docker-compose.prod.yml && docker-compose up -d
   ```

3. **Access the application**
   - Frontend: http://localhost:3000
   - Backend API: http://localhost:8000
   - MindsDB: http://localhost:47334

### Local Development Setup

1. **Clone and setup**
   ```bash
   git clone https://github.com/ritwickrajmakhal/legal-contract-analyzer.git
   cd legal-contract-analyzer
   cp .env.example .env
   # Edit .env and add your API keys
   ```

2. **Start all services**
   ```bash
   docker-compose up -d
   ```

3. **Access the application**
   - Frontend: http://localhost:3000
   - Backend API: http://localhost:8000
   - MindsDB: http://localhost:47334

## 📖 Usage

### Getting Started

1. **Launch the Application**
   - Open http://localhost:3000 in your browser
   - The app opens to a clean chat interface with suggested actions

2. **Create Your First Conversation**
   - Click "Start New Conversation" or use the sidebar to create a new chat
   - Each conversation maintains its own context for focused contract analysis

### Core Workflows

#### 1. Upload & Analyze Contracts

**Upload PDF Files:**
- **Drag & Drop**: Simply drag PDF files onto the chat composer area
- **Click to Upload**: Click the paperclip icon (📎) in the composer to browse and select PDF files
- **Paste URLs**: Paste PDF URLs directly into the chat input - the system will automatically download and process them

**Example:**
```
Upload a vendor agreement PDF, then ask: "Analyze the risk level of this contract"
```

#### 2. Connect Data Sources

**Access Integrations:**
- Click the "Manage Integrations" button in the sidebar (🔌 icon)
- Connect multiple data sources simultaneously:
  - **PostgreSQL** databases
  - **SharePoint** document libraries
  - **Dropbox** cloud storage
  - **Salesforce** CRM data
  - **Elasticsearch** search clusters
  - **Apache Solr** search platforms
  - **GitHub/GitLab** repositories
  - **Notion** workspaces
  - **Email** systems
  - **Snowflake** data warehouses

**Sync Data:**
- Click the "Sync Now" button in the header to synchronize all connected sources
- View sync status and connected source count in the header badge

#### 3. Ask Questions & Get Insights

**Natural Language Queries:**
Use the chat interface to ask questions in plain English:

```
"What are my highest-risk vendor contracts?"
"Show contracts expiring in the next 3 months"
"Compare liability clauses across all agreements"
"Find contracts with payment terms over 60 days"
```

**Quick Actions:**
- Press `Ctrl+K` (or `Cmd+K` on Mac) to access quick prompts
- Use predefined buttons for common tasks:
  - **Analyze Risks**: Get portfolio-wide risk assessment
  - **Get Timeline**: View upcoming deadlines and renewals
  - **Get Metrics**: See contract portfolio KPIs and analytics

#### 4. Interactive Analytics

**View Visual Insights:**
- **Risk Gauges**: Visual risk scoring with breakdown by category
- **Charts & Graphs**: Interactive data visualizations in chat responses
- **Timeline Views**: Contract expiration and renewal calendars
- **Metric Cards**: Key performance indicators and summaries

**Example Queries:**
```
"Show me a risk distribution chart for all contracts"
"Display contract renewal timeline for Q1 2025"
"Provide portfolio metrics and KPIs"
```

#### 5. Automated Email Actions

**Smart Email Suggestions:**
The AI automatically suggests email actions based on analysis results:
- Risk alerts requiring immediate attention
- Renewal deadline notifications
- Compliance violation reports
- Executive summaries and updates

**Execute Email Actions:**
- Click suggested email buttons within chat responses
- Choose to send immediately or schedule for later
- System tracks completed actions and provides confirmations

#### 6. Knowledge Base Management

**Access KB Manager:**
- Click the "KB Manager" button in the header
- Browse, search, and manage processed contract data
- View detailed metadata for each knowledge base entry

**Advanced Search:**
- Use semantic search to find relevant contract clauses
- Filter by source, date, or content type
- Delete outdated or incorrect entries

### Keyboard Shortcuts

- `Ctrl+K` / `Cmd+K`: Open quick prompts menu
- `Enter`: Send message
- `Shift+Enter`: New line in message
- `Escape`: Close modals or quick prompts

### Best Practices

1. **Start Specific**: Begin with focused questions about particular contract types or time periods
2. **Upload Context**: Upload relevant PDFs before asking detailed questions about them
3. **Use Quick Actions**: Leverage predefined prompts for common analysis tasks
4. **Connect Sources**: Set up data source integrations for comprehensive portfolio analysis
5. **Regular Sync**: Keep data synchronized with the "Sync Now" button
6. **Review Analytics**: Use visual insights to identify patterns and high-risk areas

## 🧪 Example Queries

### Risk Analysis
```
"Identify contracts with high liability exposure"
"Show me agreements missing termination clauses"
"Find vendors with uncapped indemnification terms"
```

### Compliance Monitoring
```
"Check GDPR compliance across data processing agreements"
"Verify all contracts have proper governing law clauses"
"Find agreements missing required insurance provisions"
```

### Financial Analysis
```
"Calculate total contract value by vendor"
"Show payment terms distribution across portfolio"
"Identify contracts with penalty clauses over $50k"
```

### Timeline Management
```
"List all contracts expiring in Q1 2025"
"Show renewal deadlines requiring 60-day notice"
"Find overdue contract renewals"
```

## 🔧 Development

### Project Structure
```
legal-contract-analyzer/
├── app/                     # Next.js pages (layout, globals, main page)
├── components/              # React UI components (chat, analytics, integrations)
├── lib/                     # TypeScript utilities and hooks
├── python-services/         # FastAPI backend
│   ├── api/                 # API routes (agent, integrations, uploads, kb)
│   ├── mindsdb_manager.py   # MindsDB integration
│   ├── kb_manager.py        # Knowledge base operations
│   └── requirements.txt     # Python dependencies
├── mdb_data/                # MindsDB persistent storage
├── docker-compose.yml       # Local development
├── docker-compose.prod.yml  # Production deployment
└── package.json            # Node.js dependencies
```

### Available Scripts

**Frontend**
```bash
npm run dev          # Start development server
npm run build        # Build for production
npm run start        # Start production server
npm run lint         # Run ESLint
npm run type-check   # TypeScript type checking
```

**Backend**
```bash
npm run python:install  # Install Python dependencies
npm run python:dev      # Start FastAPI development server
```

### API Testing

Test the backend API:
```bash
curl http://localhost:8000/health
curl -X POST http://localhost:8000/api/agent/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello, analyze my contracts"}'
```

## 🐳 Docker Deployment

### Production Deployment (Recommended)

**Using Docker Hub images (no building required):**
```bash
# Download production configuration
curl -o docker-compose.yml https://raw.githubusercontent.com/ritwickrajmakhal/legal-contract-analyzer/main/docker-compose.prod.yml

# Start all services
docker-compose up -d
```

**Using local build:**
```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Available Docker Images

The platform is published on Docker Hub with optimized production images:

- `ritwickrajmakhal/legal-contract-analyzer-frontend:latest` - Next.js frontend (~100MB)
- `ritwickrajmakhal/legal-contract-analyzer-backend:latest` - FastAPI backend (~200MB)  
- `ritwickrajmakhal/legal-contract-analyzer-mindsdb:latest` - MindsDB with integrations (~1.5GB)


**Built with ❤️ for legal professionals and contract managers**
