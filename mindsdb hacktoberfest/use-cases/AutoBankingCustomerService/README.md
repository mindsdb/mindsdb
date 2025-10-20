# AutoBanking Customer Service Workflow

An intelligent automated customer service pipeline for banking operations that leverages MindsDB AI agents to process customer interactions, classify issues, and generate actionable insights.

## 🏗️ Architecture Overview

This project implements an end-to-end automated workflow that transforms raw customer service interactions into structured, actionable business intelligence:

```
Raw Customer Service Script 
    ↓
MindsDB AI Classification Agent (Summary + Resolution Status)
    ↓
Salesforce Case Creation (All conversations)
    ↓
MindsDB Recommendation Agent (For unresolved issues only)
    ↓
Jira Issue Tickets (For unresolved issues with AI recommendations)
```

## 🔄 Workflow Pipeline

### 1. **Input Processing**
- **Source**: Raw customer service scripts (calls, chats, emails)
- **Format**: Unstructured text data from various customer touchpoints

### 2. **AI Classification (MindsDB)**
- **Classification Agent**: Analyzes conversation and determines resolution status
- **Summary Generation**: Creates concise summaries of customer interactions
- **Resolution Detection**: Identifies if issues are RESOLVED or UNRESOLVED

### 3. **CRM Integration (Salesforce)**
- **Case Creation**: Creates Salesforce cases for ALL conversations
- **Status Tracking**: Records resolution status and priority levels
- **Customer Context**: Maintains customer relationship data

### 4. **AI Recommendations (MindsDB)**
- **Recommendation Agent**: Generates actionable suggestions for UNRESOLVED issues only
- **Knowledge Base**: Uses Confluence documentation for context-aware recommendations
- **Best Practices**: Applies customer complaints handling procedures

### 5. **Issue Tracking (Jira)**
- **Ticket Creation**: Creates Jira tickets for UNRESOLVED issues only
- **AI Integration**: Includes AI recommendations in ticket descriptions
- **Priority Assignment**: Sets appropriate priority levels based on issue severity

## 🎯 Key Features

- **Intelligent Text Processing**: Advanced NLP for understanding customer intent
- **Automated Classification**: Smart categorization of customer issues
- **Sentiment Tracking**: Real-time customer satisfaction monitoring
- **Predictive Recommendations**: AI-driven suggestions for issue resolution
- **Seamless Integration**: Connects customer service data with business systems
- **Scalable Architecture**: Handles high-volume customer interactions

## 🛠️ Technology Stack

- **MindsDB**: AI/ML platform for intelligent agents and analytics
- **PostgreSQL**: Primary database for conversation storage
- **Salesforce**: CRM system for customer relationship management
- **Jira**: Issue tracking and project management
- **FastAPI**: Python web framework for API endpoints
- **Confluence**: Knowledge base for recommendation context
- **Python 3.11+**: Primary development language
- **Docker**: Containerized deployment
- **OpenAI**: AI model provider for agents

## 📊 Use Cases

### Banking Operations
- **Account Issues**: Automated routing of account-related queries
- **Loan Applications**: Intelligent pre-screening and classification
- **Fraud Detection**: Pattern recognition in customer communications
- **Compliance**: Automated documentation and audit trail generation

### Customer Experience
- **Proactive Support**: Early identification of potential issues
- **Personalized Service**: Tailored responses based on customer history
- **Satisfaction Monitoring**: Continuous tracking of customer sentiment
- **Resolution Optimization**: Data-driven improvement of service processes

## 🚀 Getting Started

### Prerequisites
- **MindsDB**: Running instance (Docker or cloud)
- **PostgreSQL**: Database server
- **Salesforce**: Developer account with API access
- **Jira**: Workspace with API token
- **Confluence**: Knowledge base access
- **Python 3.11+**: Development environment

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd AutoBankingCustomerService

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp env.template .env
# Edit .env with your API credentials
```

### Configuration

#### 1. **Database Setup**
```bash
# Start PostgreSQL (Docker)
docker run -d --name postgres-db \
  -e POSTGRES_USER=postgresql \
  -e POSTGRES_PASSWORD=psqlpasswd \
  -e POSTGRES_DB=demo \
  -p 5432:5432 postgres:13

# Initialize database schema
python3.11 script/import_banking_data.py
```

#### 2. **MindsDB Setup**
```bash
# Start MindsDB (Docker)
docker run -d --name mindsdb \
  -p 47334:47334 mindsdb/mindsdb

# Configure agents and knowledge base
# Access MindsDB web interface at http://localhost:47334
# Run the SQL commands in mindsdb_setup.sql to create:
#   - PostgreSQL database connection
#   - OpenAI ML engine
#   - Classification agent
#   - Confluence knowledge base
#   - Recommendation agent
```

#### 3. **Environment Variables**
Create a `.env` file with the following variables:

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=demo
DB_USER=postgresql
DB_PASSWORD=psqlpasswd

# MindsDB Configuration
MINDSDB_URL=http://127.0.0.1:47334

# Salesforce Configuration
SALESFORCE_USERNAME=your_username
SALESFORCE_PASSWORD=your_password
SALESFORCE_SECURITY_TOKEN=your_token
SALESFORCE_DOMAIN=your_domain

# Jira Configuration
JIRA_BASE_URL=https://your-domain.atlassian.net
JIRA_EMAIL=your_email
JIRA_API_TOKEN=your_token
JIRA_PROJECT_KEY=your_project_key
JIRA_ISSUE_TYPE=Story

# OpenAI Configuration (for MindsDB agents)
OPENAI_API_KEY=your_openai_key
```

#### 4. **Start the API Server**
```bash
python3.11 server.py
```

## 📈 Expected Benefits

### Operational Efficiency
- **80% Reduction** in manual ticket categorization time
- **60% Faster** initial response to customer issues
- **Automated Routing** to appropriate departments

### Customer Satisfaction
- **Proactive Issue Detection** before escalation
- **Personalized Responses** based on customer history
- **Faster Resolution Times** through intelligent recommendations

### Business Intelligence
- **Real-time Analytics** on customer sentiment
- **Trend Analysis** for service improvement
- **Predictive Insights** for capacity planning

## 📁 Project Structure

```
AutoBankingCustomerService/
├── app/                          # Core application code
│   ├── __init__.py              # FastAPI app initialization
│   ├── api.py                   # API routes and schemas
│   ├── db.py                    # Database utilities
│   ├── services.py              # Business logic
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
└── requirements.txt             # Python dependencies
```

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
      "conversation_text": "truncated text...",
      "summary": "AI-generated summary",
      "status": "UNRESOLVED",
      "jira_issue_key": "BCS-123",
      "jira_issue_url": "https://...",
      "salesforce_case_id": "500...",
      "salesforce_case_url": "https://...",
      "salesforce_error": null,
      "recommendation": "AI-generated recommendations",
      "recommendation_error": null,
      "created_at": "2024-01-01T00:00:00",
      "processed_at": "2024-01-01T00:00:00"
    }
  ]
}
```

## 🧪 Testing

### Test Scripts
```bash
# Test single conversation processing
python3.11 test_single_conversation.py

# Test recommendation workflow (includes AI recommendations)
python3.11 test_recommendation.py
```

### Available Test Scripts
- `test_single_conversation.py`: Basic conversation processing test
- `test_recommendation.py`: Full workflow test with AI recommendations

### Manual Testing
```bash
# Test health endpoint
curl http://localhost:8000/health

# Test conversation processing
curl -X POST http://localhost:8000/api/process-conversations \
  -H "Content-Type: application/json" \
  -d '{"conversation_texts": ["agent: Hello\nclient: Hi, I need help"]}'
```

## 📊 Monitoring & Analytics

- **Real-time Dashboard**: Monitor workflow performance and customer satisfaction
- **Alert System**: Notifications for critical issues or system failures
- **Performance Metrics**: Track processing times, accuracy rates, and resolution effectiveness

## 🔒 Security & Compliance

- **Data Encryption**: All customer data encrypted in transit and at rest
- **Access Controls**: Role-based permissions for system access
- **Audit Logging**: Comprehensive logs for compliance requirements
- **GDPR Compliance**: Data privacy and right-to-be-forgotten support

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Documentation**: [Wiki](link-to-wiki)
- **Issues**: [GitHub Issues](link-to-issues)
- **Discussions**: [GitHub Discussions](link-to-discussions)
- **Email**: support@example.com

## 🗺️ Roadmap

### Phase 1: Core Pipeline ✅ COMPLETED
- [x] Basic text processing
- [x] AI agent integration (classification + recommendation)
- [x] Salesforce connector
- [x] Jira integration
- [x] Database storage
- [x] API endpoints
- [x] Complete workflow automation
- [x] Error handling and logging
- [x] Test scripts and validation

### Phase 2: Production Ready ✅ COMPLETED
- [x] Environment configuration
- [x] Docker deployment support
- [x] Database schema management
- [x] API documentation
- [x] Comprehensive testing
- [x] Integration validation

### Phase 3: Advanced Features
- [ ] Multi-language support
- [ ] Voice-to-text integration
- [ ] Advanced analytics dashboard
- [ ] Machine learning model training
- [ ] Real-time monitoring
- [ ] Performance optimization

### Phase 4: Enterprise Features
- [ ] Multi-tenant support
- [ ] Advanced security features
- [ ] Custom AI model training
- [ ] Enterprise integrations
- [ ] High availability deployment
- [ ] Scalability enhancements

---

**Built with ❤️ for the banking industry**
