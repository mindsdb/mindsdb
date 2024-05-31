---
title: Knowledge Bases
sidebarTitle: Knowledge Bases
---

## Description

With MindsDB, you can create and deploy AI agents that comprise AI models and customizable skills such as knowledge bases and text-to-SQL.

<Tip>
Learn more about [AI Agents here](/sdks/python/agents).
</Tip>

There are two types of skills available in MindsDB:

1. Text2SQL skill translates users' questions into SQL queries and fetches relevant information from data sources assigned to this skill.
2. Knowledge Bases store data from databases, files, or webpages, and use different retrieval methods to fetch relevant information and present it as answers.

## Syntax

To feed an agent with data from different sources, use the knowledge_base skills.

Here is how to create a knowledge base:

```python
my_kb = server.knowledge_bases.create('my_kb')
```

Here is how to insert data into a knowledge base:

```python
# insert data into a knowledge base from a file
my_kb.insert(pd.read_csv('file_name.csv'))

# insert data into a knowledge base from a database
my_kb.insert(server.databases.db_name.tables.table_name.filter(column_name='value'))

# insert data into a knowledge base
my_kb.insert({'column_name': 'value', 'column_name': 'value', ...})
```

Here is how to query data from a knowledge base using specified keywords:

```python
df = my_kb.find('keyword').fetch()
```

Here is how to list all available knowledge bases:

```python
kb_list = server.knowledge_bases.list()
```

Here is how to get a knowledge base by name:

```python
kb = server.knowledge_bases.get('my_kb')
# or
kb = server.knowledge_bases.my_kb
```

Here is how to drop a knowledge base:

```python
server.knowledge_bases.drop('my_kb')
```
