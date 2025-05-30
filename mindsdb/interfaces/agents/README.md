# MindsDB Agents

This directory contains agent implementations for MindsDB.

## CrewAI Agent Pipeline

The CrewAI Agent Pipeline is a specialized agent type that uses [CrewAI](https://docs.crewai.com/) to create a pipeline of specialized agents that work together to:

1. Understand natural language queries
2. Generate SQL queries
3. Execute SQL queries
4. Validate the results

### Usage

You can create a CrewAI agent using the following SQL command:

```sql
CREATE AGENT my_crewai_agent
USING
   agent_type='crewai',
   tables=['database1.table1', 'database2.table2'],
   knowledge_base=['kb1', 'kb2'],
   provider='openai',
   model='gpt-4o',
   prompt_template='A sample prompt',
   verbose=True,
   max_tokens=200;
```

### Key Parameters

- `agent_type`: Set to 'crewai' to use the CrewAI pipeline
- `tables`: List of tables the agent can query (format: 'database.table')
- `knowledge_base`: List of knowledge bases the agent can query
- `provider`: Currently only 'openai' is supported
- `model`: The model to use (e.g. 'gpt-4o')
- `verbose`: Whether to output detailed logs
- `max_tokens`: Maximum tokens for completion

### Query Types

The CrewAI Agent can handle two types of queries:

1. **Structured Data Queries**: Converts natural language to SQL to query database tables
2. **Semantic Search Queries**: Performs semantic similarity search in knowledge bases

#### Semantic Search Example

When a query is determined to need semantic search, the agent will generate a SQL query against the knowledge base like:

```sql
SELECT id, chunk_content, relevance, distance
FROM kb_example
WHERE content = "search_term_example" AND relevance_threshold=0.6 LIMIT 50;
```

### Architecture

The CrewAI pipeline consists of four specialized agents:

1. **Query Understanding Agent**: Parses questions to identify intent and relevant data sources
2. **SQL Generation Agent**: Creates appropriate SQL statements based on the query intent
3. **SQL Execution Agent**: Safely runs the SQL and retrieves the raw results
4. **SQL Validation Agent**: Ensures results are valid, complete, and logically consistent 