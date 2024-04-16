<h1 align="center">
	<img width="300" src="https://github.com/mindsdb/mindsdb_native/blob/stable/assets/MindsDBColorPurp@3x.png?raw=true" alt="MindsDB">
	<br>
</h1>

<div align="center">

<p>
	<a href="https://pypi.org/project/MindsDB/" target="_blank"><img src="https://badge.fury.io/py/MindsDB.svg" alt="MindsDB Release"></a>
	<a href="https://www.python.org/downloads/" target="_blank"><img src="https://img.shields.io/badge/python-3.8.x%7C%203.9.x%7C%203.10.x%7C%203.11.x-brightgreen.svg" alt="Python supported"></a>
	<a href="https://ossrank.com/p/630"><img src="https://shields.io/endpoint?url=https://ossrank.com/shield/630"></a>
	<br />
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

<p align="center">
  <img src="/docs/assets/diagram.png"/>
</p>

MindsDB integrates with numerous [data sources](https://docs.mindsdb.com/integrations/data-overview), including databases, vector stores, and applications, and popular [AI/ML frameworks](https://docs.mindsdb.com/integrations/ai-overview), including AutoML and LLMs. MindsDB connects data sources with AI/ML frameworks and automates routine workflows between them. By doing so, we bring data and AI together, enabling the intuitive implementation of customized AI systems.

### Features

Common concepts and features brought by MindsDB include the following:

* **Model Management**
Manage every aspect of AI models, including creation, deployment, training, fine-tuning, and version control.

* **AI Integrations**
MindsDB gives you access to a wide range of AI frameworks within your enterprise data environment. This includes models from OpenAI, Anthropic, HuggingFace, Anyscale, and more.

* **Data Integrations**
Connect any data source to MindsDB, including databases, vector stores, and applications.

* **Automation**
Automate tasks with JOBS by scheduling execution at a defined frequency, or with TRIGGERS by defining a triggering event.

### Use Cases

MindsDB covers a wide range of use cases, including the following:

| Category    | Examples |
| -------- | ------- |
| **Automated Fine-Tuning**        | Fine-tuning of Large Language models <br> Fine-tuning of AutoML models |
| **AI Agents**                    | Knowledge bases <br> Skills <br> Agents <br> Chatbots |
| **AI-Powered Data Retrieval**    | Semantic search <br> Embeddings models <br> Recommenders |
| **Data Enrichment**              | Natural Language Processing (NLP) <br> Content generation <br> QA-driven data enrichment <br> Sentiment analysis <br> Text summarization |
| **Predictive Analytics**         | Time-series forecasting <br> Anomaly detection |
| **In-Database Machine Learning** | Automated classification models <br> Automated regression models <br> Bring Your Own Model (BYOM) to MindsDB |

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

To get started, install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop), following the instructions in linked doc pages.

| 🎯  Solutions                 | ⚙️ SQL Query Examples |
|---------------------------|-----------|
| 🤖 [Fine-Tuning](https://docs.mindsdb.com/sql/api/finetune#example-3-openai-model)            |  <code> FINETUNE mindsdb.hf_model FROM postgresql.table; </code>  |
| 📚 [Knowledge Base](https://docs.mindsdb.com/agents/knowledge-bases)         | <code> CREATE KNOWLEDGE_BASE my_knowledge FROM (SELECT contents FROM drive.files); </code> |
| 🔍 [Semantic Search](https://docs.mindsdb.com/integrations/ai-engines/rag)        |  <code> SELECT * FROM rag_model WHERE question='What product is best for treating a cold?';  </code>   |
| ⏱️ [Real-Time Forecasting](https://docs.mindsdb.com/sql/tutorials/eeg-forecasting) | <code> SELECT * FROM binance.trade_data WHERE symbol = 'BTCUSDT'; </code> |
| 🕵️ [Agents](https://docs.mindsdb.com/agents/agent)                | <code> CREATE AGENT my_agent USING model='chatbot_agent', skills = ['knowledge_base']; </code>    |
| 💬 [Chatbots](https://docs.mindsdb.com/agents/chatbot)               |  <code> CREATE CHATBOT slack_bot USING database='slack',agent='customer_support'; </code>|
| ⏲️ [Time Driven Automation](https://docs.mindsdb.com/sql/create/jobs)      |  <code> CREATE JOB twitter_bot ( <sql_query1>, <sql_query2> ) START '2023-04-01 00:00:00';   </code>           |
| 🔔 [Event Driven Automation](https://docs.mindsdb.com/sql/create/trigger)      | <code> CREATE TRIGGER data_updated ON mysql.customers_data (sql_code)           |

## ⚙️ Examples

MindsDB enables you to deploy AI/ML models, send predictions to your application, and automate AI workflows.

### AI Workflow Automation

This category of use cases involves tasks that get data from a data source, pass it through an AI/ML model, and write the output to a data destination.

<p align="center">
  <img src="/docs/assets/ai_workflow_automation.png"/>
</p>

Common use cases are anomaly detection, data indexing/labeling/cleaning, and data transformation.

This example showcases the data enrichment flow, where input data comes from a PostgreSQL database and is passed through an OpenAI model to generate new content which is saved into a data destination.

We take customer reviews from a PostgreSQL database. Then, we deploy an OpenAI model that analyzes all customer reviews and assigns sentiment values. Finally, to automate the workflow for incoming customer reviews, we create a job that generates and saves AI output into a data destination.

```sql
-- Step 1. Connect a data source to MindsDB
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

SELECT *
FROM data_source.amazon_reviews_job;

-- Step 2. Deploy an AI model
CREATE ML_ENGINE openai_engine
FROM openai
USING
    openai_api_key = 'your-openai-api-key';

CREATE MODEL sentiment_classifier
PREDICT sentiment
USING
    engine = 'openai_engine',
    model_name = 'gpt-4',
    prompt_template = 'describe the sentiment of the reviews
						strictly as "positive", "neutral", or "negative".
						"I love the product":positive
						"It is a scam":negative
						"{{review}}.":';

DESCRIBE sentiment_classifier;

-- Step 3. Join input data with AI model to get AI output
SELECT input.review, output.sentiment
FROM data_source.amazon_reviews_job AS input
JOIN sentiment_classifier AS output;

-- Step 4. Automate this workflow to accomodate real-time and dynamic data
CREATE DATABASE data_destination
WITH ENGINE = "engine-name",      -- choose the data source you want to connect to save AI output
PARAMETERS = {					  -- list of available data sources: https://docs.mindsdb.com/integrations/data-overview
    "key": "value",
	...
};

CREATE JOB ai_automation_flow (

	INSERT INTO data_destination.ai_output (
		SELECT input.created_at,
			   input.product_name,
			   input.review,
			   output.sentiment
		FROM data_source.amazon_reviews_job AS input
		JOIN sentiment_classifier AS output
		WHERE input.created_at > LAST
	);
);
```

[Discover more tutorials and use cases here](https://docs.mindsdb.com/use-cases/overview).

### AI System Deployment

This category of use cases involves creating AI systems composed of multiple connected parts, including various AI/ML models and data sources, and exposing such AI systems via APIs.

<p align="center">
  <img src="/docs/assets/ai_system_deployment.png"/>
</p>

Common use cases are agents and assistants, recommender systems, forecasting systems, and semantic search.

This example showcases AI agents, a feature developed by MindsDB. AI agents can be assigned certain skills, including text-to-SQL skills and knowledge bases. Skills provide an AI agent with input data that can be in the form of a database, a file, or a website.

We create a text-to-SQL skill based on the car sales dataset and deploy a conversational model, which are both components of an agent. Then, we create an agent and assign this skill and this model to it. This agent can be queried to ask questions about data stored in assigned skills.

```sql
-- Step 1. Connect a data source to MindsDB
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

SELECT *
FROM data_source.car_sales;

-- Step 2. Create a skill
CREATE SKILL my_skill
USING
    type = 'text2sql',
    database = 'data_source',
    tables = ['car_sales'],
    description = 'car sales data of different car types';

SHOW SKILLS;

-- Step 3. Deploy a conversational model
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
    user_column = 'question' ,
    assistant_column = 'answer',
    max_tokens = 100,
    temperature = 0,
    verbose = True,
    prompt_template = 'Answer the user input in a helpful way';

DESCRIBE my_conv_model;

-- Step 4. Create an agent
CREATE AGENT my_agent
USING
    model = 'my_conv_model',
    skills = ['my_skill'];

SHOW AGENTS;

-- Step 5. Query an agent
SELECT *
FROM my_agent
WHERE question = 'what is the average price of cars from 2018?';

SELECT *
FROM my_agent
WHERE question = 'what is the max mileage of cars from 2017?';

SELECT *
FROM my_agent
WHERE question = 'what percentage of sold cars (from 2016) are automatic/semi-automatic/manual cars?';

SELECT *
FROM my_agent
WHERE question = 'is petrol or diesel more common for cars from 2019?';

SELECT *
FROM my_agent
WHERE question = 'what is the most commonly sold model?';
```

[Agents are accessible via API endpoints](https://docs.mindsdb.com/rest/agents/agent).

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
