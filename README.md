<a name="readme-top"></a>

  <br />
    <a href="https://github.com/mindsdb/mindsdb">
    <img src="/assets/mindsdb-header-github.png" alt="MindsDB" width=100%>

  <br />
  <br />
	
<div align="center">
	<a href="https://pypi.org/project/MindsDB/" target="_blank"><img src="https://badge.fury.io/py/MindsDB.svg" alt="MindsDB Release"></a>
	<a href="https://www.python.org/downloads/" target="_blank"><img src="https://img.shields.io/badge/python-3.10.x%7C%203.11.x%7C%203.12.x%7C%203.13.x-brightgreen.svg" alt="Python supported"></a>
	<a href="https://hub.docker.com/u/mindsdb" target="_blank"><img src="https://img.shields.io/docker/pulls/mindsdb/mindsdb" alt="Docker pulls"></a>

  <br />


  <p align="center">
    <br />
    <a href="https://mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Website</a>
    ·
    <a href="https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Docs</a>
    ·
    <a href="https://mindsdb.com/contact?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Contact us for a Demo</a>
    ·
    <a href="https://mindsdb.com/joincommunity?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Community Slack</a>
  </p>
</div>

----------------------------------------

MindsDB is the most widely adopted Query Engine for AI agents and LLMs that need to answer questions directly from Databases, Data Warehouses and Applications — no ETL required.

<div align="center">
<a href="https://www.youtube.com/watch?v=HN4fHtS4mvo" target="_blank">
  <img src="/assets/mindsdb_demo.gif" alt="MindsDB Demo" width=80%>
</a>
</div>

## What can you build with MindsDB Query Engine

<br>
<table border="1" cellspacing="0" cellpadding="16" style="border-collapse: collapse; width: 100%;">
  <tr>
    <td width="50%" valign="top">
      <strong>TEXT-TO-SQL AGENTS</strong><br><br>
      Get precise, data-driven answers instantly, using natural language.<br><br>
      Unify and answer questions from all your data (MySQL, Salesforce, Shopify, etc), skip ETL.<br><br>
      <a href="https://www.youtube.com/watch?v=QIdPpzcaxXg">Watch video</a>
    </td>
    <td width="50%" valign="top">
      <strong>SEMANTIC SEARCH AGENTS</strong><br><br>
      Augment LLMs' responses with your most relevant information.<br><br>
      Semantically search across unstructured data sources like documents, Jira tickets, Google Drive, etc.<br><br>
      <a href="https://www.youtube.com/watch?v=HN4fHtS4mvo">Watch video</a>
    </td>
  </tr>
</table>

## How MindsDB works

The core philosophy of MindsDB—Connect, Unify, Respond — mirrors the workflow of a developer building agents. At the center lies MindsDB’s unified SQL-compatible data language, enriched with additional constructs that enable the search of unstructured data, manage data workflows (including jobs and triggers), and more.

<table style="width:100%; border-collapse:collapse; border:none;">
  <tr>
    <td style="width:25%; border:none; padding:8px 16px; vertical-align:middle;">
      <strong><a href="https://docs.mindsdb.com/integrations/data-overview?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Connect</a></strong>
    </td>
    <td style="width:75%; border:none; padding:8px 16px; vertical-align:middle;">
      <strong>Universal Data Access:</strong>
      Give your agents universal read access to 200+ live data sources (Postgres, MongoDB, Slack, Files).
    </td>
  </tr>
  <tr>
    <td style="width:25%; border:none; padding:8px 16px; vertical-align:middle;">
      <strong><a href="https://docs.mindsdb.com/mindsdb-unify?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Unify</a></strong>
    </td>
    <td style="width:75%; border:none; padding:8px 16px; vertical-align:middle;">
      <strong>Dynamic Context Engine:</strong>
      Fuse structured tables with vectorized data (text, PDFs, HTML) inside a Knowledge Base.
    </td>
  </tr>
  <tr>
    <td style="width:25%; border:none; padding:8px 16px; vertical-align:middle;">
      <strong><a href="https://docs.mindsdb.com/mindsdb-respond?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Respond</a></strong>
    </td>
    <td style="width:75%; border:none; padding:8px 16px; vertical-align:middle;">
      <strong>Autonomous Reasoning:</strong>
      Deploy agents that don't just "retrieve" but "reason" by joining data points across your entire stack.
    </td>
  </tr>
</table>

## Quickstart - Build Your First Agent in 5 minutes

Install MindsDB via Docker, or Docker Desktop (<a href="https://docs.mindsdb.com/setup/self-hosted/docker?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">docs</a>).
```bash
docker run --name mindsdb_container \
  -e MINDSDB_APIS=http,mysql \
  -p 47334:47334 -p 47335:47335 \
  mindsdb/mindsdb
# open http://127.0.0.1:47334
```

Connect and blend live data from various sources, such as Postgres and MongoDB (<a href="https://docs.mindsdb.com/mindsdb-connect?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">docs</a>).
```sql
--1. Connect SQL database
CREATE DATABASE postgres_demo
WITH ENGINE = 'postgres', 
PARAMETERS = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "samples.mindsdb.com",
    "port": "5432",
    "database": "demo",
    "schema": "sample_data"
};

--2. Connect document database
CREATE DATABASE mongodb_demo
WITH
  ENGINE = 'mongodb',
  PARAMETERS = {
    "host": "mongodb+srv://demouser:MindsDB_demo@mindsdb-demo.whljnvh.mongodb.net/public-demo"
  };

--3. Blend MongoDB and Postgres data into a single view
CREATE VIEW mindsdb.enterprise_sales AS (
  SELECT *
  FROM postgres_demo.websales_sales AS sales
  JOIN mongodb_demo.customers AS customers
     ON sales.customer_id = customers.customer_id
  WHERE
     customers.segment = "Enterprise"
     AND sales.sales > 1000
);
```

Automatically vectorize unstructured data (reviews), blending it with structured metadata (<a href="https://docs.mindsdb.com/mindsdb_sql/knowledge_bases/overview?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">docs</a>).
  <br /> (*Note: Install a ChromaDB <a href="https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">dependency</a> in the container before running the commands below.*)
```sql
-- 4. Create a knowledge base
CREATE KNOWLEDGE BASE reviews_kb
USING
  content_columns  = ['review_text'],
  metadata_columns = ['review_id','product_id','customer_id','rating','review_date'];

-- 5. Insert data into a knowledge base
INSERT INTO reviews_kb
SELECT review_text, review_id, product_id, customer_id, rating, review_date
FROM mongodb_demo.reviews;
```

Ask questions in natural language (<a href="https://docs.mindsdb.com/mindsdb_sql/agents/agent?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">docs</a>).
```sql
-- 6. Create an agent
CREATE AGENT my_agent
USING
data = { 
   "knowledge_bases": ["mindsdb.reviews_kb"],
   "tables": ['mindsdb.enterprise_sales']
},
prompt_template='you are a skilled AI analyst. Provide accurate responces based on the data available to you:
"mindsdb.reviews_kb" has data about product reviews
"mindsdb.enterprise_sales" has data about sales to enterprise customers
';

-- 7. Query agent
SELECT answer
FROM my_agent 
WHERE question = 'What do enterprise customers say about our best selling product?';
```

Integrate it into your own application via API or MCP (<a href="https://docs.mindsdb.com/overview_sdks_apis?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">docs</a>).

## 📃 Tutorials
- Enterprise Knowledge Search (<a href="https://mindsdb.com/blog/fast-track-knowledge-bases-how-to-build-semantic-ai-search-by-andriy-burkov?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">example</a>)
- Advanced Semantic Search  (<a href="https://mindsdb.com/blog/blend-hybrid-retrieval-with-structured-data-using-mindsdb-knowledge-bases?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">example</a>)
- Customer Support Automation (<a href="https://mindsdb.com/blog/building-janus-an-ai-powered-customer-support-helpdesk-system-powered-by-mindsdb?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">example1</a>, <a href="https://mindsdb.com/blog/building-agentic-workflow-auto-banking-customer-service-with-mindsdb?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">example2</a>)
- Intelligent Content Discovery (<a href="https://mindsdb.com/blog/mysql-mindsdb-unlocks-intelligent-content-discovery-for-web-cms-with-knowledge-bases-and-cursor?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">example</a>)
- Financial Analysis Agents (<a href="https://mindsdb.com/blog/streamline-financial-analysis-with-mindsdb-s-knowledge-bases-and-hybrid-search?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">example</a>)
- Real-time AI-powered analytics (<a href="https://mindsdb.com/blog/mariadb-mindsdb-turns-woocommerce-data-to-insights-with-real-time-ai-analytics-for-ecommerce-teams?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">example</a>)
- Conversational Data Assistants (<a href="https://mindsdb.com/blog/unlocking-operational-intelligence-in-energy-utilities-with-mindsdb-knowledge-bases-hybrid-search?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">example</a>)
- CRM Intelligence (<a href="https://mindsdb.com/blog/unlocking-salesforce-crm-intelligence-with-mindsdb-s-ai-powered-knowledge-bases?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">example</a>)
- Compliance & Customer Intelligence (<a href="https://mindsdb.com/blog/enterprise-software-vendors-drive-compliance-customer-insights-with-mindsdb-knowledge-bases-hybrid-search?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">example</a>)
- Conversation Intelligence (<a href="https://mindsdb.com/blog/introducing-mindsdb-s-integration-with-gong-ai-analytics-on-call-data?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">example</a>)
Subscribe to our (<a href="https://mindsdb.com/blog?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">blog</a>) for more

## 🫴 Help and support

Stuck on a query? Found a bug? We’re here to help.
<table style="width:100%; border-collapse:collapse;">
  <tr>
    <td style="width:30%; border:1px solid #d0d7de; padding:12px; vertical-align:top;">
      Ask a question
    </td>
    <td style="width:70%; border:1px solid #d0d7de; padding:12px; vertical-align:top;">
      Join our <a href="https://mindsdb.com/joincommunity">Slack Community</a>.
    </td>
  </tr>
  <tr>
    <td style="width:30%; border:1px solid #d0d7de; padding:12px; vertical-align:top;">
      Report a bug
    </td>
    <td style="width:70%; border:1px solid #d0d7de; padding:12px; vertical-align:top;">
      Open a <a href="https://github.com/mindsdb/mindsdb/issues">GitHub Issue</a>. Please include reproduction steps!
    </td>
  </tr>
  <tr>
    <td style="width:30%; border:1px solid #d0d7de; padding:12px; vertical-align:top;">
      Get commercial support
    </td>
    <td style="width:70%; border:1px solid #d0d7de; padding:12px; vertical-align:top;">
      Contact the <a href="https://mindsdb.com/contact?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">MindsDB Team</a> for enterprise SLAs and custom solutions.
    </td>
  </tr>
</table>

Security Note: If you find a security vulnerability, please do not open a public issue. Refer to our <a href="https://github.com/mindsdb/mindsdb/security">Security Policy</a> for reporting instructions.

## 🤝 Contribute to MindsDB

MindsDB is an open-source project, and we love contributions! Whether you are fixing a bug, adding a new data integration, or building a new AI agent skill, your help is welcome.

<strong>Ways you can help:</strong>
- 🔌 Add a Data Integration: We support 100+ sources, but there is always room for more. Help us connect to new databases, SaaS APIs, or vector stores.
- 🧠 Add an AI Handler: Integrate the latest LLMs or specialized models into the MindsDB reasoning engine.
- 📚 Improve Documentation: Help fellow developers by clarifying guides, fixing typos, or adding examples.
- 🐛 Report Bugs: Found an issue? Let us know so we can make the engine more robust.

<strong>Ready to start?</strong>

- Read our <a href="https://docs.mindsdb.com/contribute/contribute?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Contribution Guide</a> to get set up.
- Check out <a href="https://github.com/mindsdb/mindsdb/issues">Open Issues</a> to find a task to tackle.
- Join the #contributors channel on <a href="https://mindsdb.com/joincommunity">Slack</a> to discuss ideas.
- Check out our <a href="https://mindsdb.com/community?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">community rewards and programs</a>.

<div align="center">

<strong>Our top 100 contributors</strong>

<a href="https://github.com/mindsdb/mindsdb/graphs/contributors">
<img src="https://contrib.rocks/image?repo=mindsdb/mindsdb&max=100&columns=10" />
</a>
	
Made with [contrib.rocks](https://contrib.rocks)
</div>

## 📚 Useful Resources

- Step-by-step guides, product news (<a href="https://mindsdb.com/blog?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">subscribe to blog</a>)
- Demos, live webinars with experts (<a href="https://mindsdb.com/events?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">events</a>)
- Community and support (<a href="https://mindsdb.com/joincommunity">Slack</a>)
- Brand guidelines (<a href="https://mindsdb.com/press-kit?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">press-kit</a>)
- Contact us (<a href="https://mindsdb.com/contact?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">form</a>)

