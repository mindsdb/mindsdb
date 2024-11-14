

<a name="readme-top"></a>

<div align="center">
	<a href="https://pypi.org/project/MindsDB/" target="_blank"><img src="https://badge.fury.io/py/MindsDB.svg" alt="MindsDB Release"></a>
	<a href="https://www.python.org/downloads/" target="_blank"><img src="https://img.shields.io/badge/python-3.9.x%7C%203.10.x%7C%203.11.x-brightgreen.svg" alt="Python supported"></a>
	<a href="https://ossrank.com/p/630"><img src="https://shields.io/endpoint?url=https://ossrank.com/shield/630"></a>
	<img alt="PyPI - Downloads" src="https://img.shields.io/pypi/dm/Mindsdb">
	<a href="https://hub.docker.com/u/mindsdb" target="_blank"><img src="https://img.shields.io/docker/pulls/mindsdb/mindsdb" alt="Docker pulls"></a>

  <br />
  <br />

  <a href="https://github.com/mindsdb/mindsdb">
    <img src="/docs/assets/mindsdb_logo.jpg" alt="MindsDB" width="300">
  </a>

  <p align="center">
    <br />
    <a href="https://www.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Website</a>
    ·
    <a href="https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Docs</a>
    ·
    <a href="https://mindsdb.com/joincommunity?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Community Slack</a>
  </p>
</div>

----------------------------------------

[MindsDB](https://mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) is a platform for building AI solutions that can learn from and answer questions over federated data.

[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=The%20platform%20for%20building%20AI,%20from%20enterprise%20data&url=https://github.com/mindsdb/mindsdb&via=mindsdb&hashtags=ai,opensource)

## 📖 About Us

MindsDB is a powerful federated AI query engine that allows developers to connect agents and AI applications to a wide variety of [data sources](https://docs.mindsdb.com/integrations/data-overview?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

## 🚀 Get Started

* **[Install MindsDB](https://docs.mindsdb.com/setup/self-hosted/docker-desktop?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo)** using [Docker](https://docs.mindsdb.com/setup/self-hosted/docker?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).
* **[Connect Your Data](https://docs.mindsdb.com/mindsdb_sql/sql/create/database)** — Connect and query hundreds of different [data sources](https://docs.mindsdb.com/integrations/data-overview?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).
* **[Prepare Your Data](https://docs.mindsdb.com/use-cases/data_enrichment/overview)** — Prepare, [organize](https://docs.mindsdb.com/mindsdb_sql/sql/create/view), and [automate](https://docs.mindsdb.com/mindsdb_sql/sql/create/jobs) data transformations using AI and ML to fit your needs.

## 🎯 Use Cases

After [connecting](https://docs.mindsdb.com/mindsdb_sql/sql/create/database) and [preparing](https://docs.mindsdb.com/use-cases/data_enrichment/overview) your data, you can leverage MindsDB to implement the following use cases:

| 🎯 Use Case                 | ⚙️ Description | Python SDK | SQL |
|---------------------------|-----------|---------| ----- |
| [RAG](https://docs.mindsdb.com/agents/knowledge-bases?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo)  | Comprehensive RAG that can be populated from numerous data sources | [(Python)](https://docs.mindsdb.com/sdks/python/agents_knowledge_bases) | [(SQL)](https://docs.mindsdb.com/mindsdb_sql/agents/knowledge-bases) |
| [Agents](https://docs.mindsdb.com/agents/agent?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo)  | Equip agents to answer questions over structured and unstructured data in MindsDB | [(Python)](https://docs.mindsdb.com/sdks/python/agents) | [(SQL)](https://docs.mindsdb.com/mindsdb_sql/agents/agent) |
| [Automation](https://docs.mindsdb.com/mindsdb_sql/sql/create/jobs) | Automate AI-data workflows using Jobs | [(Python)](https://docs.mindsdb.com/sdks/python/create_job) | [(SQL)](https://docs.mindsdb.com/mindsdb_sql/sql/create/jobs) |

## 💡 Examples

### AI System Deployment

Create AI systems composed of multiple connected components and expose them via APIs.

<p align="center">
  <img src="/docs/assets/ai_system_deployment.png"/>
</p>

A common use case involves connecting agents to data. The following example shows how to create an AI agent capable of searching over structured data:

```sql
-- Step 1: Connect a data source to MindsDB
CREATE DATABASE data_source
WITH ENGINE = "postgres",
PARAMETERS = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "samples.mindsdb.com",
    "port": "5432",
    "database": "demo",
    "schema": "demo_data"
};

SELECT * FROM data_source.car_sales;

-- Step 2: Create a skill
CREATE SKILL my_skill
USING
    type = 'text2sql',
    database = 'data_source',
    tables = ['car_sales'],
    description = 'car sales data of different car types';

SHOW SKILLS;

-- Step 3: Deploy a conversational model
CREATE ML_ENGINE langchain_engine
FROM langchain
USING
      openai_api_key = 'your openai-api-key';
      
CREATE MODEL my_conv_model
PREDICT answer
USING
    engine = 'langchain_engine',
    model_name = 'gpt-4',
    mode = 'conversational',
    user_column = 'question',
    assistant_column = 'answer',
    max_tokens = 100,
    temperature = 0,
    verbose = True,
    prompt_template = 'Answer the user input in a helpful way';

DESCRIBE my_conv_model;

-- Step 4: Create an agent
CREATE AGENT my_agent
USING
    model = 'my_conv_model',
    skills = ['my_skill'];

SHOW AGENTS;

-- Step 5: Query an agent
SELECT * FROM my_agent WHERE question = 'What is the average price of cars from 2018?';
```

[Agents are also accessible via API endpoints](https://docs.mindsdb.com/rest/agents/agent?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

## 🤝 Contribute

Interested in contributing to MindsDB? Follow our [installation guide for development](https://docs.mindsdb.com/contribute/install?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

You can find our [contribution guide here](https://docs.mindsdb.com/contribute/contribute?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

We welcome suggestions! Feel free to open new issues with your ideas, and we’ll guide you.

This project adheres to a [Contributor Code of Conduct](https://github.com/mindsdb/mindsdb/blob/main/CODE_OF_CONDUCT.md). By participating, you agree to follow its terms.

Also, check out our [community rewards and programs](https://mindsdb.com/community?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

## 🤍 Support

If you find a bug, please submit an [issue on GitHub](https://github.com/mindsdb/mindsdb/issues/new/choose).

Here’s how you can get community support:

* Ask a question in our [Slack Community](https://mindsdb.com/joincommunity?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).
* Join our [GitHub Discussions](https://github.com/mindsdb/mindsdb/discussions).
* Post on [Stack Overflow](https://stackoverflow.com/questions/tagged/mindsdb) with the MindsDB tag.

For commercial support, please [contact the MindsDB team](https://mindsdb.com/contact?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

## 💚 Current Contributors

<a href="https://github.com/mindsdb/mindsdb/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=mindsdb/mindsdb" />
</a>

Generated with [contributors-img](https://contributors-img.web.app).

## 🔔 Subscribe for Updates

Join our [Slack community](https://mindsdb.com/j
