<h1 align="center">
	<img width="300" src="https://github.com/mindsdb/mindsdb_native/blob/stable/assets/MindsDBColorPurp@3x.png?raw=true" alt="MindsDB">
	<br>
</h1>

<div align="center">

<p>
	<a href="https://github.com/mindsdb/mindsdb/releases"><alt="MindsDB Release"></a>
	<a href="https://www.python.org/downloads/" target="_blank"><img src="https://img.shields.io/badge/python-3.8.x%7C%203.9.x%7C%203.10.x%7C%203.11.x-brightgreen.svg" alt="Python supported"></a>
	<a href="https://ossrank.com/p/630"><img src="https://shields.io/endpoint?url=https://ossrank.com/shield/630"></a>
	<br />
	<a href="https://pypi.org/project/MindsDB/" target="_blank"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>
	<img alt="PyPI - Downloads" src="https://img.shields.io/pypi/dm/Mindsdb">
	<a href="https://hub.docker.com/u/mindsdb" target="_blank"><img src="https://img.shields.io/docker/pulls/mindsdb/mindsdb" alt="Docker pulls"></a>
	<br />
	
</p>

<h3 align="center">
	<a href="https://www.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Website</a>
	<span> | </span>
	<a href="https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Docs</a>
	<span> | </span>
	<a href="https://mindsdb.com/joincommunity">Community Slack</a>
	<span> | </span>
	<a href="https://github.com/mindsdb/mindsdb/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22">Contribute</a>
</h3>

</div>

----------------------------------------

## About us

MindsDB is the platform for customizing AI from enterprise data.

With MindsDB, you can deploy, serve, and fine-tune models in real-time, utilizing data from databases, vector stores, or applications, to build AI-powered apps - using universal tools developers already know.



MindsDB integrates with numerous [data sources](https://docs.mindsdb.com/integrations/data-overview), including databases, vector stores, and applications, and popular [AI/ML frameworks](https://docs.mindsdb.com/integrations/ai-overview), including AutoML and LLMs. MindsDB connects data sources with AI/ML frameworks and automates routine workflows between them. By doing so, we bring data and AI together, enabling the intuitive implementation of customized AI systems.

### Features

Common concepts and features brought by MindsDB include the following:

**Model Management**
Manage every aspect of AI models, including creation, deployment, training, fine-tuning, and version control.

**AI Integrations**
MindsDB gives you access to a wide range of AI frameworks within your enterprise data environment. This includes models from OpenAI, Anthropic, HuggingFace, Anyscale, and more.

**Data Integrations**
Connect any data source to MindsDB, including databases, vector stores, and applications.

**Automation**
Automate tasks with JOBS by scheduling execution at a defined frequency, or with TRIGGERS by defining a triggering event.

### Use Cases

MindsDB covers a wide range of use cases, including the following:

**Automated Fine-Tuning**
- Fine-tuning of Large Language models
- Fine-tuning of AutoML models

**AI Agents**
- Knowledge bases
- Skills
- Agents
- Chatbots

**AI-Powered Data Retrieval**
- Semantic search
- Embeddings models
- Recommenders

**Data Enrichment**
- Natural Language Processing (NLP)
- Content generation
- QA-driven data enrichment
- Sentiment analysis
- Text summarization

**Predictive Analytics**
- Time-series forecasting
- Anomaly detection

**In-Database Machine Learning**
- Automated classification models
- Automated regression models
- Bring Your Own Model (BYOM) to MindsDB

[Discover tutorials and use cases here](https://docs.mindsdb.com/use-cases/overview).

## Get Started

To get started, install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Extension](https://docs.mindsdb.com/setup/self-hosted/docker-desktop), following the instructions in linked doc pages.

## ⚙️ Example 

MindsDB enables you to deploy AI/ML models and send predictions to your application.

This example deploys an OpenAI model via the RAG handler provided by MindsDB.

```sql
-- Step 1. Configure an OpenAI engine, providing your OpenAI API key.
CREATE ML_ENGINE rag_engine
FROM rag
USING
    openai_api_key = 'sk-xxx';

-- Step 2. Create and deploy a model trained on one of the MindsDB’s doc pages.
CREATE MODEL mindsdb_rag_model
PREDICT answer
USING
   engine = "rag_engine",
   llm_type = "openai",
   url = 'https://docs.mindsdb.com/what-is-mindsdb',
   vector_store_folder_name = 'db_connection',
   input_column = 'question'; 

-- Step 3. Ask questions about MindsDB.
SELECT *
FROM mindsdb_rag_model
WHERE question = 'What ML use cases does MindsDB support?';
```

[Discover more tutorials and use cases here](https://docs.mindsdb.com/use-cases/overview).

## 🤝 Contribute

If you’d like to contribute to MindsDB, install MindsDB for development following [this instruction](https://docs.mindsdb.com/contribute/install).

You’ll find the [contribution guide here](https://docs.mindsdb.com/contribute/contribute).

We are always open to suggestions, so feel free to open new issues with your ideas, and we can guide you!

This project is released with a [Contributor Code of Conduct](https://github.com/mindsdb/mindsdb/blob/stable/CODE_OF_CONDUCT.md). By participating in this project, you agree to follow its terms.

Also, check out the [rewards and community programs here](https://mindsdb.com/community).

## 🤍 Support

If you find a bug, please submit an [issue on GitHub here](https://github.com/mindsdb/mindsdb/issues/new/choose).

Here is how you can get community support:

* Post a question at [MindsDB Slack Community](https://mindsdb.com/joincommunity).
* Ask for help at our [GitHub Discussions](https://github.com/mindsdb/mindsdb/discussions).
* Ask a question at [Stackoverflow](https://stackoverflow.com/questions/tagged/mindsdb) with a MindsDB tag.

If you need commercial support, please [contact the MindsDB team](https://mindsdb.com/contact).

### 💚 Current contributors

<a href="https://github.com/mindsdb/mindsdb/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=mindsdb/mindsdb" />
</a>

Made with [contributors-img](https://contributors-img.web.app).

## 🔔 Subscribe to updates

Join our [Slack community](https://mindsdb.com/joincommunity) and subscribe to the monthly [Developer Newsletter](https://mindsdb.com/newsletter) to get product updates, information about MindsDB events and contests, and useful content, like tutorials.


## ⚖️ License 

For detailed licensing information, please refer to the [LICENSE file](https://github.com/mindsdb/mindsdb/blob/master/LICENSE).
