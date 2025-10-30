# 🚀 Setup Guide - Customer Support Intelligence

Complete step-by-step guide to set up and run this project.

## Prerequisites

### 1. MindsDB Account

**Option A: MindsDB Cloud (Recommended for beginners)**
1. Go to https://cloud.mindsdb.com
2. Sign up for a free account
3. Note your email and password for `.env` file

**Option B: Local MindsDB**
1. Install MindsDB locally:
   ```bash
   pip install mindsdb
   ```
2. Start MindsDB:
   ```bash
   python -m mindsdb
   ```
3. Access at http://127.0.0.1:47334

### 2. Python Environment

- Python 3.8 or higher
- pip package manager

## Installation Steps

### Step 1: Clone/Download Project

```bash
# If from GitHub
git clone <repository-url>
cd use-cases/customer-support-intelligence

# Or download and extract ZIP file
```

### Step 2: Create Virtual Environment (Recommended)

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

This installs:
- `mindsdb-sdk` - MindsDB Python SDK
- `pandas` - Data manipulation
- `jupyter` - Interactive notebooks
- `matplotlib`, `seaborn` - Visualization
- `python-dotenv` - Environment variables
- `tabulate` - Pretty tables

### Step 4: Configure Environment Variables

1. Copy the example file:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your credentials:
   ```env
   # For MindsDB Cloud
   MINDSDB_EMAIL=your-email@example.com
   MINDSDB_PASSWORD=your-password

   # OR for local MindsDB
   # MINDSDB_URL=http://127.0.0.1:47334
   ```

### Step 5: Verify Installation

```bash
python -c "import mindsdb_sdk; print('✅ MindsDB SDK installed')"
python -c "import pandas; print('✅ Pandas installed')"
```

## Running the Project

### Option 1: Python Demo Script (Quickest)

```bash
python demo_script.py
```

This will:
1. Connect to MindsDB
2. Load the support tickets data
3. Create Knowledge Base
4. Ingest data
5. Run semantic search demos
6. Run hybrid search demos
7. Evaluate KB performance

**Expected output:**
```
🎃 MINDSDB HACKTOBERFEST 2025
Customer Support Intelligence with Knowledge Bases
================================================================================

🔗 Connecting to MindsDB...
✅ Connected successfully

📊 Loading support tickets data...
✅ Loaded 25 tickets

📚 Setting up Knowledge Base: support_tickets_kb...
✅ Created new Knowledge Base

📥 Ingesting data into Knowledge Base...
✅ Successfully ingested 25 tickets

🔍 SEMANTIC SEARCH DEMO
...
```

### Option 2: Jupyter Notebook (Interactive)

```bash
jupyter notebook customer_support_kb.ipynb
```

Then:
1. Run cells sequentially (Shift + Enter)
2. Explore and modify queries
3. Visualize results

### Option 3: Python Interactive Shell

```python
import mindsdb_sdk
import pandas as pd

# Connect
server = mindsdb_sdk.connect(login='your-email', password='your-password')

# Load data
df = pd.read_csv('data/support_tickets.csv')
print(df.head())

# Create KB
kb = server.knowledge_bases.create(
    name='support_tickets_kb',
    model='openai',
    storage='chromadb'
)

# Search
results = kb.search("login problems", limit=3)
for r in results:
    print(r['content'])
```

## Troubleshooting

### Issue: "ModuleNotFoundError: No module named 'mindsdb_sdk'"

**Solution:**
```bash
pip install mindsdb-sdk
```

### Issue: "Authentication failed"

**Solution:**
- Verify your email/password in `.env`
- Check if you're using the correct MindsDB Cloud credentials
- Try logging in at https://cloud.mindsdb.com to verify

### Issue: "Knowledge Base already exists"

**Solution:**
```python
# Get existing KB instead of creating new one
kb = server.knowledge_bases.get('support_tickets_kb')
```

### Issue: "Connection timeout"

**Solution:**
- Check your internet connection
- If using local MindsDB, ensure it's running:
  ```bash
  python -m mindsdb
  ```

### Issue: "Rate limit exceeded"

**Solution:**
- Wait a few minutes before retrying
- MindsDB Cloud has rate limits for free tier
- Consider upgrading or using local MindsDB

## Next Steps

### 1. Customize the Dataset

Replace `data/support_tickets.csv` with your own data:
- Keep the same column structure
- Or modify the ingestion code to match your schema

### 2. Connect to Real Data Sources

MindsDB supports many integrations:

```python
# PostgreSQL
server.databases.create(
    name='postgres_db',
    engine='postgres',
    connection_args={
        'host': 'localhost',
        'port': 5432,
        'database': 'support_db',
        'user': 'user',
        'password': 'password'
    }
)

# Then query directly
results = kb.search("SELECT * FROM postgres_db.tickets")
```

### 3. Build a Web Interface

Create a simple Flask/FastAPI app:

```python
from flask import Flask, request, jsonify
import mindsdb_sdk

app = Flask(__name__)
server = mindsdb_sdk.connect(...)
kb = server.knowledge_bases.get('support_tickets_kb')

@app.route('/search', methods=['POST'])
def search():
    query = request.json['query']
    results = kb.search(query, limit=5)
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
```

### 4. Set Up Automated Updates

Create a job to update KB periodically:

```sql
CREATE JOB support_kb_updater (
    INSERT INTO support_tickets_kb
    SELECT * FROM new_tickets
    WHERE created_date > LAST_RUN_TIME
)
EVERY 1 hour;
```

### 5. Add More Advanced Features

- **Agent Integration**: Add conversational AI
- **Multi-language Support**: Translate queries
- **Custom Embeddings**: Use domain-specific models
- **Real-time Updates**: WebSocket notifications

## Testing

### Test Semantic Search

```python
test_queries = [
    "login problems",
    "billing issues",
    "slow performance",
    "feature requests"
]

for query in test_queries:
    results = kb.search(query, limit=1)
    print(f"Query: {query}")
    print(f"Top result: {results[0]['metadata']['ticket_id']}\n")
```

### Test Hybrid Search

```python
# Test different filter combinations
filters_to_test = [
    {'category': 'Technical'},
    {'priority': 'Critical'},
    {'status': 'Resolved'},
    {'category': 'Billing', 'status': 'Resolved'}
]

for filters in filters_to_test:
    results = kb.search("problems", limit=5, filters=filters)
    print(f"Filters: {filters}")
    print(f"Results: {len(results)}\n")
```

## Performance Optimization

### 1. Batch Ingestion

Instead of inserting one-by-one:

```python
# Prepare all data first
data_batch = []
for _, row in df.iterrows():
    data_batch.append({
        'content': prepare_content(row),
        'metadata': prepare_metadata(row)
    })

# Insert in batch
kb.insert(data_batch)
```

### 2. Caching

Cache frequent queries:

```python
from functools import lru_cache

@lru_cache(maxsize=100)
def cached_search(query, limit=5):
    return kb.search(query, limit=limit)
```

### 3. Indexing

Ensure proper metadata indexing for faster filtering.

## Resources

- **MindsDB Docs**: https://docs.mindsdb.com
- **Knowledge Bases**: https://docs.mindsdb.com/knowledge-bases
- **Python SDK**: https://docs.mindsdb.com/sdks/python
- **Community Slack**: https://mindsdb.com/joincommunity

## Support

- **GitHub Issues**: [Create an issue]
- **MindsDB Community**: https://mindsdb.com/joincommunity
- **Email**: [Your email]

---

**Happy Hacking! 🎃**
