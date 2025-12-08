sql_description = """
MindsDB SQL is compatible with DuckDB syntax.
When writing the SQL query, make sure the select renames the columns accordingly to the question.

Example:
SELECT * FROM somedb.movies WHERE whatever...;

This is a valid SQL query, but its best to rename the columns to something more descriptive.

SELECT movie_id, movie_description, age, name FROM somedb.movies WHERE whatever...;
"""

sql_with_kb_description = """

MindsDB SQL is compatible with DuckDB syntax, with additional features for knowledge bases.

When the question requires to filter by something semantically, use the knowledge bases available when possible.
You can determine what knowledge bases are relevant given teh data catalog.

Example:
Knowledge Base Metadata:
kb,kb_insert_query,parent_query_id_column,parent_query_content_columns,parent_query_metadata_columns
mindsdb.somekb,"INSERT INTO somekb SELECT movie_id AS `id`, movie_description AS content FROM somedb.movies",id,content,age, name

This tells you that somekb is a knowledge base that was created from the movies table 
and that you can use it so search information about the movies descriptions, which is better than 
trying to do keyword search on the movies table. For that here is a detailed description of how to use knowledge bases.

**Knowledge Base Queries:**
Knowledge bases are semantic search tables that allow you to find relevant entries using semantic text search and metadata filtering.

Example queries:
- Semantic search:  
  `SELECT * FROM mindsdb.kb_name WHERE content LIKE 'your semantic search query' AND relevance >= 0.5`
- Metadata filtering:  
  `SELECT * FROM mindsdb.kb_name WHERE metadata_column = 'value'`
- Combined semantic and metadata filtering:  
  `SELECT * FROM mindsdb.kb_name WHERE content LIKE 'your semantic search query' AND metadata_column = 'value' AND relevance >= 0.5`

Where output columns will be: id,chunk_id,chunk_content,<one or more metadata columns>,relevance,distance
use relevance >0.5 to filter for relevant results.

From the knowledge base, you can identify where id came from, and what content comes from, so when you SELECT, you can rename columns accordingly.

For example, to find up to 10,000 movies that are excellent, not horror, and have an age group of PG-13 or higher (assuming a metadata column named "age"):
instead of searching the movies table seamantically SELECT * FROM somedb.movies WHERE age >= 13 AND description LIKE '%excellent%' LIMIT 10000;
which is prone to missing results as it is likely to miss results where people write similar things to exellent and horror, but in a different way.
as such, you can search the knowledge base, which does not require any exact matches, it will filter by most relevant results. 
```
SELECT 
   id as movie_id,
   chunk_id as movie_description_chunk_id,
   chunk_content as movie_description_chunk,
   age as rated_age, -- assuming age is a metadata column
   name as movie_name, -- assuming name is a metadata column
   relevance
FROM mindsdb.movies_kb
WHERE content LIKE 'excellent, not horror' AND age >= 13 AND relevance >= 0.5
LIMIT 10000;
```


or since there can be multiple chunks for a movie that match the query you can aggregate results:

```
SELECT
   id AS movie_id,
   LIST(chunk_id) AS movie_description_chunk_ids,
   LIST(chunk_content) AS movie_description_chunks,
   MAX(relevance) AS max_relevance,  -- the most relevant chunk
   age AS rated_age, -- assuming age is a metadata column, same for each id
   name AS movie_name -- assuming name is a metadata column, same for each id
FROM mindsdb.movies_kb
WHERE content LIKE 'excellent, not horror' AND age >= 13 AND relevance >= 0.5
GROUP BY id, age, name
ORDER BY max_relevance DESC
LIMIT 10000;
```


Suppose you have a table `movies_data` from which the knowledge base was created. To answer: "Show the count of excellent, non-horror, PG-13+ movies by release year":

```
SELECT release_year, COUNT(*) FROM db.movies_data
WHERE movie_id IN (
    SELECT DISTINCT id FROM mindsdb.movies_kb
    WHERE content LIKE 'excellent, not horror' AND age >= 13 AND relevance >= 0.5 LIMIT 10000
)
GROUP BY release_year;
```
- NEVER IN KNOWLEDGE BASE QUERIES USE content  ILIKE  (ilike is not suppoerted)

- AVOID: JOINS between tables and knowledge bases, instead use WHERE <IDCOLUMN> IN (SELECT DISTINCT id FROM knowledge_base) ...

- ALWAYS: It is important to set an appropriate LIMIT on knowledge base queries to avoid missing results; the default limit is 10, so if you need more than 10, set it accordingly.

- When writing the SQL query, make sure the select renames the columns accordingly to the question.

"""

markdown_instructions = """
**IMPORTANT FORMATTING REQUIREMENTS:**
- Always format your responses in Markdown
- When presenting tabular data or query results, organize them as Markdown tables
- Use proper Markdown table syntax with headers and aligned columns
- For example:
| Column1 | Column2 | Column3 |
|---------|---------|---------|
| Value1  | Value2  | Value3  |
- Use other Markdown formatting (headers, lists, code blocks) as appropriate to make responses clear and well-structured
"""
      