# Database Setup Scripts

This directory contains utility scripts for initializing the PostgreSQL database.

## Scripts

### 1. Database initializer (`app.db`)

Initializes the `conversations_summary` table for the MindsDB AI agent workflow.

**Features:**
- Automatically checks if table exists before creating
- Can be used standalone or imported as a module
- Used by `server.py` for automatic initialization on startup

**Usage:**

```bash
# Run standalone to create table
python -m app.db

# Import as module (used by server startup)
from app.db import ensure_table_exists
ensure_table_exists()
```

**Creates:**
- `conversations_summary` table with columns:
  - `id` (SERIAL PRIMARY KEY)
  - `conversation_id` (VARCHAR, UNIQUE)
  - `conversation_text` (TEXT)
  - `summary` (TEXT, NULL) - Filled by MindsDB agent
  - `resolved` (BOOLEAN, NULL) - Filled by MindsDB agent
  - `created_at`, `updated_at` (TIMESTAMP)
- Indexes on `conversation_id` and `resolved`
- PostgreSQL trigger for automatic `updated_at` updates

---

### 2. `import_banking_data.py`

Imports banking customer service conversation CSV data into PostgreSQL and preprocesses conversations.

**CSV Data Format:**

The script expects CSV files with the following columns:
- `conversation_id` - Unique conversation identifier
- `speaker` - Speaker type (agent/client)
- `date_time` - Timestamp of the message
- `text` - Message content

**Available Datasets:**

- **banking_sample_10k.csv** (1.9 MB) - 10,000 records for testing
- **banking_300k.csv** (1.0 GB) - 300,000+ records for production

> **Note**: Place CSV files in the parent directory (`AutoBankingCustomerService/`)

**Usage:**

```bash
# Sample dataset (recommended for testing)
python script/import_banking_data.py

# Full dataset
python script/import_banking_data.py --full

# Custom CSV file
python script/import_banking_data.py --file path/to/your.csv
```

**What It Does:**

1. Creates PostgreSQL schema and tables
2. Imports raw conversation messages into `banking_conversations`
3. Preprocesses conversations:
   - Aggregates messages by conversation_id
   - Sorts messages by timestamp
   - Calculates conversation statistics
4. Creates `banking_conversations_preprocessed` table

---

## Requirements

- PostgreSQL running on `localhost:5432`
- Database: `demo`
- User: `postgresql` / Password: `psqlpasswd`
- Python packages: `psycopg2`

## Database Connection

Both scripts use the same default configuration:

```python
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "demo",
    "user": "postgresql",
    "password": "psqlpasswd"
}
```

## Automatic Initialization

The FastAPI server (`app/__init__.py`) automatically calls `ensure_table_exists()` on startup, so manual initialization of the `conversations_summary` table is optional.
