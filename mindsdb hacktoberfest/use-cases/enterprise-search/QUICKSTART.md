# Area51 Quick Start

**Required**
1. Jira and confluence API key
2. Zendesk API key
3. Azure open AI credentials

**Setup (one-time)**

```bash

# 0. Copy the example environment file and add the required parameters for the datasources
cp .env.example .env

# 1. Install dependencies
uv sync

# 2. Start Docker containers
docker-compose up -d

# 3. Run setup script
uv run python setup.py --mode setup

# 4. Open MindsDB UI
open http://localhost:47334
```

**Run Evaluations**
```bash
# 1. Create test tables first
uv run python utils/create_test_tables.py

# 2. Run evaluation
uv run python utils/evaluate_kb.py

# Or use MindsDB Studio with the queries in README.md
```

