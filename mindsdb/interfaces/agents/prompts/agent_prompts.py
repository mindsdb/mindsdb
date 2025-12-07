sql_tool_description = """
Use this tool to answer questions about data in the database.

Execute MindsDB SQL queries on database tables and knowledge bases. MindsDB SQL is compatible with DuckDB syntax, with additional features for knowledge bases.

**Knowledge Base Queries:**
Knowledge bases are semantic search tables that allow you to find relevant entries using semantic text search and metadata filtering.

Example queries:
- Semantic search:  
  `SELECT * FROM mindsdb.kb_name WHERE content LIKE 'your semantic search query' AND relevance >= 0.5`
- Metadata filtering:  
  `SELECT * FROM mindsdb.kb_name WHERE metadata_column = 'value'`
- Combined semantic and metadata filtering:  
  `SELECT * FROM mindsdb.kb_name WHERE content LIKE 'your semantic search query' AND metadata_column = 'value' AND relevance >= 0.5`

For example, to find up to 10,000 movies that are excellent, not horror, and have an age group of PG-13 or higher (assuming a metadata column named "age"):

```
SELECT * FROM mindsdb.movies_kb
WHERE content LIKE 'excellent, not horror' AND age >= 13 AND relevance >= 0.5
LIMIT 10000;
```

Suppose you have a table `movies_data` from which the knowledge base was created. To answer: "Show the count of excellent, non-horror, PG-13+ movies by release year":

```
SELECT release_year, COUNT(*) FROM db.movies_data
WHERE movie_id IN (
    SELECT id FROM mindsdb.movies_kb
    WHERE content LIKE 'excellent, not horror' AND age >= 13 AND relevance >= 0.5 LIMIT 10000
)
GROUP BY release_year;
```

**Note:** It is important to set an appropriate LIMIT on knowledge base queries; otherwise, the default limit is 10.

**Important Guidelines:**
- Always execute the SQL query using this tool to answer questions.

"""