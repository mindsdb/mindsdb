---
title: AI Agents' Skills
sidebarTitle: Skills
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

Here is how to create a new skill:

```python
text_to_sql_skill = skills.create('text_to_sql', 'sql', { 'tables': ['my_table'], 'database': 'my_database' })
```

<Note>
Note that it is required to assign a database and a set of tables to be used by this skill.
</Note>

Here is how to list all available skills:

```python
skills = skills.list()
```

Here is how to get an existing skill by name:

```python
skill = skills.get('my_skill')
```

Here is how to update a skill with new datasets:

```python
skill.params = { 'tables': ['new_table'], 'database': 'new_database' }
updated_skill = skills.update('my_skill', skill)
```

Here is how to delete a skill:

```python
skills.delete('my_skill')
```

Here is how to create an agent and assign it a skill:

```python
agent = agents.create('my_agent', model_name, [text_to_sql_skill])
```
