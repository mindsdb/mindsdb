---
title: Moss
sidebarTitle: Moss
---

In this section, we present how to connect Moss to MindsDB.

[Moss](https://moss.dev) is a semantic search runtime built for Conversational AI agents. It lets you index documents and run hybrid semantic/keyword queries in under 10ms — fast enough for real-time conversation, compared to 200–300ms with typical retrieval systems.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Moss to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).
3. Create a Moss project and obtain your credentials from the [Moss Portal](https://portal.usemoss.dev).

## Connection

This handler is implemented using the `moss` Python library.

To connect your Moss project to MindsDB, use the following statement:

```sql
CREATE DATABASE moss_datasource
WITH ENGINE = 'moss',
PARAMETERS = {
    "project_id": "your-project-id",
    "project_key": "moss_access_key_xxxxx",
    "alpha": "0.8"
};
```

The required parameters are:

* `project_id`: Your Moss project ID, available in the [Moss Portal](https://portal.usemoss.dev).
* `project_key`: Your Moss project access key.

The optional parameters are:

* `alpha`: Controls the blend between semantic and keyword search. Range is `0.0` to `1.0`, where `0.0` is pure keyword (BM25), `1.0` is pure semantic, and `0.8` is the default.

## Usage

Once connected, you can insert documents into a Moss index. The index is created automatically on the first insert.

```sql
INSERT INTO moss_datasource.my_index (id, content, metadata)
VALUES
    ('doc-1', 'MindsDB unifies AI and data pipelines', '{"category": "product"}'),
    ('doc-2', 'Connect MindsDB to PostgreSQL, MySQL, and more', '{"category": "integrations"}'),
    ('doc-3', 'Create AI models using the CREATE MODEL syntax', '{"category": "docs"}');
```

<Note>
The `INSERT` statement blocks until Moss finishes building the index, which typically takes 5–30 seconds depending on document count.
</Note>

To run a semantic search query:

```sql
SELECT id, content, distance
FROM moss_datasource.my_index
WHERE content = 'how do I create an AI model?'
LIMIT 5;
```

The `distance` column is `1 - score`, so lower values indicate a closer match.

To fetch all documents without a search query:

```sql
SELECT id, content, metadata
FROM moss_datasource.my_index;
```

To fetch specific documents by ID:

```sql
SELECT id, content
FROM moss_datasource.my_index
WHERE id = 'doc-1';
```

To filter results by metadata alongside a semantic search:

```sql
SELECT id, content, distance
FROM moss_datasource.my_index
WHERE content = 'connecting to databases'
  AND metadata.category = 'integrations'
LIMIT 3;
```

To delete documents from an index:

```sql
DELETE FROM moss_datasource.my_index
WHERE id = 'doc-1';
```

To drop an index entirely:

```sql
DROP TABLE moss_datasource.my_index;
```

## Using Moss in a RAG Pipeline

You can combine Moss with a MindsDB model to build a retrieval-augmented generation (RAG) pipeline entirely in SQL:

```sql
SELECT r.content, m.answer
FROM moss_datasource.my_index AS r
JOIN mindsdb.my_llm AS m
WHERE r.content = 'what is the refund policy?'
LIMIT 1;
```
