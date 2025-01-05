

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


MindsDB is the world's most effective solution for building AI applications that talk to messy enterprise data sources. Think of it as a librarian Marie Kondo.

<p align="center">
  <img src="/docs/assets/cloud/main_mdb.png"/>
</p>

A federated query engine that tidies up your data-sprawl chaos while meticulously answering every single question you throw at it. From structured to unstructured data, whether it's scattered across SaaS applications, databases, or... hibernating in data warehouses like that $100 bill in your tuxedo pocket from prom night, lost, waiting to be discovered.

## Install MindsDB Server 

MindsDB is an open-source server that can be deployed anywhere - from your laptop to the cloud, and everywhere in between. And yes, you can customize it to your heart's content.


  * [Using Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop). This is the fastest and recommended way to get started and have it all running.
  * [Using Docker](https://docs.mindsdb.com/setup/self-hosted/docker). This is also simple, but gives you more flexibility on how to further customize your server.

## [Connect Your Data](https://www.mindsdb.com/integrations/data-overview)

You can connect to hundreds of [data sources (learn more)](https://docs.mindsdb.com/integrations/data-overview). This is just an example of a Postgres database.

```sql

-- Connect to demo postgres DB
CREATE DATABASE demo_postgres_db
WITH ENGINE = "postgres",
PARAMETERS = {
  "user": "demo_user",
  "password": "demo_password",
  "host": "samples.mindsdb.com",
  "port": "5432",
  "database": "demo",
  "schema": "demo_data"
};

```

Once you've connected your data sources, you can [combine](https://docs.mindsdb.com/mindsdb_sql/sql/api/join-on), [slice it, dice it](https://docs.mindsdb.com/mindsdb_sql/sql/api/select), and [transform](https://docs.mindsdb.com/use-cases/data_enrichment/overview) it however your heart desires using good ol' standard SQL [(learn more)](https://docs.mindsdb.com/mindsdb_sql/sql/create/view). 

After you've whipped your data into shape, it's time to build AI that actually learns!

## Build AI Knowledge

Our Knowledge Bases are state-of-the-art autonomous RAG systems that can digest data from any source MindsDB supports. Whether your data is structured and neater than a Swiss watch factory or unstructured and messy as a teenager's bedroom, our Knowledge Base engine will figure out how to find the relevant information. 


**In this example** we will create a knowledge base that knows everything about amazon reviews. 


```sql

-- first you create a knowledge base
CREATE KNOWLEDGE_BASE mindsdb.reviews_kb;


-- now we can insert everything from the amazon reviews table into it, so it can learn it
INSERT INTO mindsdb.reviews_kb (
  SELECT review as content FROM demo_pg_db.amazon_reviews
);

-- You can check the status of your loads here
SELECT * FROM information_schema.knowledge_bases;

```

For the tinkerers and optimization enthusiasts out there, you can dive as deep as you want. [(Learn more about knowledge Bases)](https://docs.mindsdb.com/mindsdb_sql/agents/knowledge-bases)

+ Want to [hand-pick your embedding model? Go for it](https://docs.mindsdb.com/mindsdb_sql/agents/knowledge-bases#knowledge-base-with-openai-embedding-model)! 
+ Have strong [opinions about vector databases? We're here for it!](https://docs.mindsdb.com/mindsdb_sql/agents/knowledge-bases#knowledge-base-with-custom-vector-store). 

But if you'd rather spend your time on other things (like finally building that billion-dollar AI App), that's perfectly fine too. By default, it's all handled automatically - you don't need to worry about the nitty-gritty details like data embedding, chunking, vector optimization, etc.

## Search 

Now that your knowledge base is loaded and ready. Let's hunt for some juicy info!

#### Via SQL
```sql
-- Find the reviews that about Iphone in beast of lights

SELECT *  FROM mindsdb.reviews_kb
WHERE content LIKE 'what are the best kindle reviews'
LIMIT 10;

```

#### Via Python SDK

Install MindsDB SDK

```shell
pip install mindsdb_sdk
```

You can call this AI knowledge base from your app with the following code:

```python
import mindsdb_sdk


# connects to the specified host and port
server = mindsdb_sdk.connect('http://127.0.0.1:47334')

wiki_kb = server.knowledge_bases.get('mindsdb.reviews_kb');
df = my_kb.find('what are the best kindle reviews').fetch()

```
