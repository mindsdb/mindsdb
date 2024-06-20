---
title: AI Agents with LLMs and Skills
sidebarTitle: Agents
---

## Description

With MindsDB, you can create and deploy AI agents that comprise AI models and customizable skills such as knowledge bases and text-to-SQL.

<p align="center">
  <img src="/assets/agent_diagram.png" />
</p>

AI agents comprise of skills, such as text2sql and knowledge_base, and a conversational model.

* Skills provide data resources to an agent, enabling it to answer questions about available data. Learn more about [skills here](/sdks/python/agents_skills). Learn more about [knowledge bases here](/sdks/python/agents_knowledge_bases).

* A conversational model (like OpenAI) from LangChain utilizes [tools as skills](https://python.langchain.com/docs/modules/agents/tools/) to respond to user input. Users can customize these models with their own prompts to fit their use cases.

## Syntax

### Creating an agent

When creating an agent, you can use the default conversational model:

```python
agent = server.agents.create(f'new_demo_agent')
```

Or specify model parameters:

```python
agent = server.agents.create(
    f'new_demo_agent',
    model={
        'model_name': 'gpt-4',
        'openai_api_key': 'MY_OPENAI_API_KEY',
        'prompt_template': 'Hello! Ask a question: {{question}}'
        'temperature': 0.0,
        'max_tokens': 1000,
        'top_p': 1.0,
        'top_k': 0,
        ...
    }
)
```

Or use an existing model:

```python
model = server.models.get('existing_model')
agent = server.agents.create('demo_agent', model)
```

Furthermore, you can list all existing agents, get agents by name, update agents, and delete agents.

```python
# list all agents
agents = agents.list()

# get an agent by name
agent = agents.get('my_agent')

# update an agent
new_model = models.get('new_model')
agent.model_name = new_model.name
new_skill = skills.create('new_skill', 'sql', { 'tables': ['new_table'], 'database': 'new_database' })
updated_agent.skills.append(new_skill)
updated_agent = agents.update('my_agent', agent)

# delete an agent by name
agents.delete('my_agent')
```

### Assigning skills to an agent

You can add skills to an agent, providing it with data stored in databases, files, or webpages.

The retrieval skill is similar to [knowledge bases](/sdks/python/agents_knowledge_bases).

```python
# add data from one or more files as skills
agent.add_file('./file_name.txt', 'file content description')
agent.add_files(['./file_name.pdf', './file_name.pdf', ...], 'files content description')

# add data from one or more webpages as skills
agent.add_webpages(['example1.com', 'example2.com', ...], 'webpages content description')
```

The text2SQL skill retrieves relevant information from databases.

```python
# add data from a database connected to mindsdb as skills
db = server.databases.create('datasource_name', 'engine_name', {'database': 'db.db_name'})
db = server.databases.get('datasource_name')
agent.add_database(db.name, ['table_name'], 'data description')
```

<Tip>
Learn more about [all data sources integrated with MindsDB](/integrations/data-overview).
</Tip>

### Querying an agent

Once you created an agent and assigned it a set of skills, you can ask questions related to your data.

```python
question = 'ask a question'
answer = agent.completion([{'question': question, 'answer': None}])
print(answer.content)
```

## Example

Here is a sample Python code to deploy an agent:

```python
import mindsdb_sdk
con = mindsdb_sdk.connect()

# IMPORTANT: This code requires to set OPENAI_API_KEY as env variable

agent = con.agents.create(f'new_demo_agent')

print('Adding Hooblyblob details...')
agent.add_file('./hooblyblob.txt', 'Details about the company Hooblyblob')

print('Adding rulebook details...')
agent.add_files(['./codenames-rulebook.pdf'], 'Rulebooks for various board games')

print('Adding MindsDB docs...')
agent.add_webpages(['docs.mindsdb.com'], 'Documentation for MindsDB')

print('Agent ready to use.')
while True:
    print('Ask a question: ')
    question = input()
    answer = agent.completion([{'question': question, 'answer': None}])
    print(answer.content)
```
