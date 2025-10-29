# MindsDB Banking Customer Service - Important Notes

## MindsDB Syntax Constraints

### UPDATE Statement - No AS Keyword for Table Alias

**WRONG ❌:**
```sql
UPDATE banking_postgres_db.conversations_summary AS target
SET summary = 'test'
WHERE target.id = 1;
```

**Error:**
```
Syntax error, unknown input:
>UPDATE banking_postgres_db.conversations_summary AS target
--------------------------------------------------^^
```

**CORRECT ✅:**
```sql
UPDATE banking_postgres_db.conversations_summary target
SET summary = 'test'
WHERE target.id = 1;
```

**Key Point:** MindsDB does NOT support the `AS` keyword when creating table aliases in UPDATE statements. Use the table alias directly without `AS`.

---

## TRIGGER vs JOB

### TRIGGER Limitations

MindsDB TRIGGER **only supports INSERT operations**, NOT UPDATE operations.

**What TRIGGER can do:**
```sql
CREATE TRIGGER my_trigger
ON database.table_name
(
    INSERT INTO another_table (col1, col2)
    SELECT col1, col2
    FROM TABLE_DELTA
    WHERE condition
);
```

**What TRIGGER CANNOT do:**
```sql
-- ❌ This will fail!
CREATE TRIGGER my_trigger
ON database.table_name
(
    UPDATE database.table_name target
    SET col1 = 'value'
    WHERE target.id = 1
);
```

### JOB for UPDATE Operations

For UPDATE operations on external database tables, use **CREATE JOB**:

```sql
CREATE JOB process_new_conversations (
    UPDATE banking_postgres_db.conversations_summary target
    SET
        summary = (
            SELECT answer
            FROM classification_agent
            WHERE question = target.conversation_text
            LIMIT 1
        ),
        resolved = CASE
            WHEN (
                SELECT answer
                FROM classification_agent
                WHERE question = target.conversation_text
                LIMIT 1
            ) LIKE '%Status: RESOLVED%' THEN TRUE
            ELSE FALSE
        END
    WHERE
        target.summary IS NULL
)
EVERY 10 seconds;
```

**Important Notes:**
1. No `AS` keyword for table alias (`target`, not `AS target`)
2. No `COLUMNS` clause needed in JOB
3. No `TABLE_DELTA` in JOB (use direct WHERE conditions)
4. Cannot use `created_at > LAST` in UPDATE WHERE clause (LAST only works in SELECT statements)

---

## Working Example: Complete Setup

### Step 1: Create Database Connection
```sql
CREATE DATABASE banking_postgres_db
WITH ENGINE = 'postgres',
PARAMETERS = {
    "host": "localhost",
    "port": 5432,
    "database": "demo",
    "user": "postgresql",
    "password": "psqlpasswd",
    "schema": "demo_data"
};
```

### Step 2: Create Classification Agent
```sql
CREATE AGENT classification_agent
USING
    model = {
        "provider": "openai",
        "model_name": "gpt-4o",
        "api_key": "your-openai-api-key"
    },
    prompt_template = '
You are a banking customer service analyst. Analyze the conversation and provide:

1. A concise summary (2-3 sentences)
2. Classification: RESOLVED or UNRESOLVED

Conversation:
{{question}}

Format your response EXACTLY as:
Summary: [your summary]
Status: [RESOLVED or UNRESOLVED]
    ';
```

### Step 3: Create JOB to Auto-Process Conversations
```sql
CREATE JOB process_new_conversations (
    UPDATE banking_postgres_db.conversations_summary target
    SET
        summary = (
            SELECT answer
            FROM classification_agent
            WHERE question = target.conversation_text
            LIMIT 1
        ),
        resolved = CASE
            WHEN (
                SELECT answer
                FROM classification_agent
                WHERE question = target.conversation_text
                LIMIT 1
            ) LIKE '%Status: RESOLVED%' THEN TRUE
            ELSE FALSE
        END
    WHERE
        target.summary IS NULL
)
EVERY 10 seconds;
```

### Verify Setup
```sql
-- Check JOB status
SHOW JOBS;

-- Check JOB execution history
SELECT * FROM log.jobs_history
WHERE name = 'process_new_conversations'
ORDER BY created_at DESC
LIMIT 10;

-- Test agent directly
SELECT answer
FROM classification_agent
WHERE question = 'agent: Hello\nclient: I need help with my account';
```

---

## Troubleshooting

### Issue: JOB not processing records

**Check:**
1. Verify JOB is active: `SHOW JOBS;`
2. Check execution logs: `SELECT * FROM log.jobs_history WHERE name = 'process_new_conversations' ORDER BY created_at DESC;`
3. Test agent manually: `SELECT answer FROM classification_agent WHERE question = 'test';`
4. Verify records exist with NULL summary: `SELECT COUNT(*) FROM banking_postgres_db.conversations_summary WHERE summary IS NULL;`

### Issue: Syntax errors

**Common mistakes:**
- Using `AS` keyword in UPDATE: Use `UPDATE table alias` instead of `UPDATE table AS alias`
- Using `LAST` in UPDATE WHERE: `LAST` only works in SELECT statements
- Using TRIGGER for UPDATE: Use JOB instead of TRIGGER for UPDATE operations

---

## Performance Optimization

### Adjusting JOB Frequency

```sql
-- Fast processing (every 10 seconds)
EVERY 10 seconds;

-- Moderate processing (every minute)
EVERY 1 minute;

-- Batch processing (every hour)
EVERY 1 hour;
```

### Limiting Agent Calls

To avoid duplicate agent calls in the same UPDATE, consider storing the agent result in a variable (if MindsDB supports it), or accept the duplicate calls as a trade-off for simplicity.

**Current approach (calls agent twice per record):**
```sql
SET
    summary = (SELECT answer FROM classification_agent WHERE question = target.conversation_text LIMIT 1),
    resolved = CASE
        WHEN (SELECT answer FROM classification_agent WHERE question = target.conversation_text LIMIT 1) LIKE '%RESOLVED%'
        THEN TRUE ELSE FALSE
    END
```

**Note:** This calls the agent twice for each record. MindsDB should optimize this internally, but if performance is an issue, consider using a MODEL instead of an AGENT, or implement processing in the application layer.

---

## Alternative Approach: Application-Layer Processing

If MindsDB JOB has limitations or performance issues, process in your application (server.py):

```python
def process_with_mindsdb_agent(conversation_id: str, conversation_text: str):
    """Query MindsDB agent and update PostgreSQL"""
    # Connect to MindsDB via PostgreSQL protocol
    mindsdb_conn = psycopg2.connect(
        host="127.0.0.1",
        port=47335,
        database="mindsdb",
        user="mindsdb",
        password=""
    )

    with mindsdb_conn.cursor() as cur:
        cur.execute(
            "SELECT answer FROM classification_agent WHERE question = %s",
            (conversation_text,)
        )
        result = cur.fetchone()

        if result:
            agent_answer = result[0]
            summary = agent_answer
            resolved = 'Status: RESOLVED' in agent_answer

            # Update PostgreSQL
            pg_conn = get_db_connection()
            with pg_conn.cursor() as pg_cur:
                pg_cur.execute(
                    "UPDATE demo_data.conversations_summary SET summary = %s, resolved = %s WHERE conversation_id = %s",
                    (summary, resolved, conversation_id)
                )
                pg_conn.commit()
```

---

## Summary

✅ **DO:**
- Use JOB for UPDATE operations
- Omit `AS` keyword in UPDATE table aliases
- Use `WHERE target.summary IS NULL` to avoid reprocessing
- Schedule JOB with `EVERY X seconds/minutes/hours`

❌ **DON'T:**
- Use TRIGGER for UPDATE operations (only INSERT supported)
- Use `AS` keyword in UPDATE statements
- Use `LAST` in UPDATE WHERE clauses
- Expect TRIGGER to automatically update the same table it's triggered on

---

**Last Updated:** 2025-10-17
