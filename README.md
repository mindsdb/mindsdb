<h1 align="center">
	<img width="300" src="https://github.com/mindsdb/mindsdb_native/blob/stable/assets/MindsDBColorPurp@3x.png?raw=true" alt="MindsDB">
	<br>
</h1>
<h4 align="center"> <blockquote> With our AI SQL Server, the future of data isn't just a possibility, it's in your SQL queries, happening in real-time. </blockquote> </h4>

<div align="center">

<a
     href="https://runacap.com/ross-index/annual-2022/"
     target="_blank"
     rel="noopener"
/>
    <img
        style="width: 260px; height: 56px"
        src="https://runacap.com/wp-content/uploads/2023/02/Annual_ROSS_badge_white_2022.svg"
        alt="ROSS Index - Fastest Growing Open-Source Startups | Runa Capital"
        width="260"
        height="56"
    />
</a>

<p>
	<a href="https://github.com/mindsdb/mindsdb/actions"><img src="https://github.com/mindsdb/mindsdb/actions/workflows/release.yml/badge.svg" alt="MindsDB Release"></a>
	<a href="https://www.python.org/downloads/" target="_blank"><img src="https://img.shields.io/badge/python-3.8.x%7C%203.9.x%7C%203.10.x%7C%203.11.x-brightgreen.svg" alt="Python supported"></a>
	<a href="https://pypi.org/project/MindsDB/" target="_blank"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>
	<br />
	<img alt="PyPI - Downloads" src="https://img.shields.io/pypi/dm/Mindsdb">  <a href="https://hub.docker.com/u/mindsdb" target="_blank"><img src="https://img.shields.io/docker/pulls/mindsdb/mindsdb" alt="Docker pulls"></a>
	<a href="https://ossrank.com/p/630"><img src="https://shields.io/endpoint?url=https://ossrank.com/shield/630"></a>
	<a href="https://www.mindsdb.com/"><img src="https://img.shields.io/website?url=https%3A%2F%2Fwww.mindsdb.com%2F" alt="MindsDB Website"></a>
	<a href="https://mindsdb.com/joincommunity" target="_blank"><img src="https://img.shields.io/badge/slack-@mindsdbcommunity-brightgreen.svg?logo=slack " alt="MindsDB Community"></a>
	<br />
	<a href="https://deepnote.com/project/Machine-Learning-With-SQL-8GDF7bc7SzKlhBLorqoIcw/%2Fmindsdb_demo.ipynb" target="_blank"><img src="https://deepnote.com/buttons/launch-in-deepnote-white.svg" alt="Launch in Deepnote"></a>
	<br />
	<a href="https://gitpod.io/#https://github.com/mindsdb/mindsdb" target="_blank"><img src="https://gitpod.io/button/open-in-gitpod.svg" alt="Open in Gitpod"></a>
</p>

<h3 align="center">
	<a href="https://www.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Website</a>
	<span> | </span>
	<a href="https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Docs</a>
	<span> | </span>
	<a href="https://mindsdb.com/joincommunity">Community Slack</a>
	<span> | </span>
	<a href="https://github.com/mindsdb/mindsdb/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22">Contribute</a>
	<span> | </span>
	<a href="https://hashnode.com/hackathons/mindsdb">Hashnode Hackathon</a>
</h3>

</div>

----------------------------------------


[MindsDB's](https://mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) **AI SQL Server** enables developers to build **AI tools** that need access to **real-time data** to perform their tasks.
 [![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Build%20AI-Centered%20Applications%20&url=https://www.mindsdb.com&via=mindsdb&hashtags=ai,ml,nlp,machine_learning,neural_networks,databases,gpt3)

| 🎯  Features                 | ⚙️ SQL Query Examples |
|---------------------------|-----------|
| 🤖 Fine-Tuning            |  <code> FINETUNE mindsdb.hf_model FROM postgresql; </code>  |
| 📚 Knowledge Base         | <code> CREATE KNOWLEDGE_BASE my_knowledge USING model=embedding_model; </code> |
| 🔍 Semantic Search        |  <code> SELECT * FROM rag WHERE question='What product is best for treating a cold?';  </code>   |
| ⏱️ Real-Time Forecasting | <code> SELECT * FROM binance.trade_data WHERE symbol = 'BTCUSDT'; </code> |
| 🕵️ Agents                | <code> CREATE AGENT my_agent USING model='chatbot_agent', skills = ['test_skill']; </code>    |
| 💬 Chatbots               |  <code> CREATE CHATBOT slack_bot USING database='slack',agent='customer_support'; </code>|
| ⏲️ Time Driven Automation      |  <code> CREATE JOB twitter_bot ( <sql_query1>, <sql_query2> ) START '2023-04-01 00:00:00';   </code>           |
| 🔔 Event Driven Automation      | <code> CREATE TRIGGER data_updated ON mysql.customers_data (sql_code)           |

## ⚡️ Quick Example

<img src='https://docs.google.com/drawings/d/e/2PACX-1vTlROMTlXiYUecoAogwjBVI0eQDYWWI-aY5npcxVjfLzGL6Fs2-YN-aOcUeWFCDzZDxveYe5Dxwilia/pub?w=1438&h=703'></img>

```sql
---This query creates a job in MindsDB to analyze Shopify orders.
---It predicts customer engagement scores based on recent completed orders
---and inserts these insights into a customer_engagement table.
---The job runs every 12 hours, providing ongoing updates to the engagement scores.

CREATE JOB mindsdb.shopify_customer_engagement_job AS (

   -- Insert into a table insights about customer engagement based on recent Shopify orders
   INSERT INTO shopify_insights.customer_engagement (customer_id, predicted_engagement_score)
      SELECT
         o.customer_id AS customer_id,
         r.predicted_engagement_score AS predicted_engagement_score
      FROM shopify_data.orders o
      JOIN mindsdb.customer_engagement_model r
         WHERE
            o.order_date > LAST
         AND o.status = 'completed'
      LIMIT 100
)
EVERY 12h;

```

## ⚙️ Installation <a name="Installation"></a>

To install locally or on-premise, pull the latest [Docker image](https://hub.docker.com/r/mindsdb/mindsdb/tags?page=1&ordering=last_updated):

```
docker pull mindsdb/mindsdb
```

or, use [pip](https://pypi.org/project/MindsDB/):

```
pip install mindsdb
```

[Read more about Installation](https://docs.mindsdb.com/setup/self-hosted/docker)



## 🔗 Data Integrations <a name="DatabaseIntegrations"></a>

MindsDB allows querying hundreds of data sources, such as databases (both relational and non-relational), data warehouses, streams, and SaaS application data, using standard SQL. This capability stems from MindsDB’s unique ability to translate SQL into real-time data requests. You can find the list of all supported integrations [here](https://docs.mindsdb.com/data-integrations/all-data-integrations).


[:question: :wave: Missing integration?](https://github.com/mindsdb/mindsdb/issues/new?assignees=&labels=&template=feature-mindsdb-request.yaml)


## 📖 Documentation <a name="Documentation"></a>

You can find the complete documentation of MindsDB at [docs.mindsdb.com](https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

## 🤍 Support <a name="Support"></a>

If you found a bug, please submit an [issue on GitHub](https://github.com/mindsdb/mindsdb/issues/new/choose).

To get community support, you can:

* Post a question at MindsDB [Slack community](https://mindsdb.com/joincommunity).
* Ask for help at our [GitHub Discussions](https://github.com/mindsdb/mindsdb/discussions).
* Ask a question at [Stackoverflow](https://stackoverflow.com/questions/tagged/mindsdb) with a MindsDB tag.

If you need commercial support, please [contact](https://mindsdb.com/contact/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) MindsDB team.

## 🤝 Contributing <a name="Contributing"></a>

A great place to start contributing to MindsDB is to check our GitHub projects :checkered_flag:

* Community contributor's [dashboard tasks](https://github.com/mindsdb/mindsdb/projects/8).
* [First timers only issues](https://github.com/mindsdb/mindsdb/issues?q=is%3Aissue+is%3Aopen+label%3Afirst-timers-only), if this is your first time contributing to an open source project.

We are always open to suggestions, so feel free to open new issues with your ideas, and we can guide you!

Being part of the core team is accessible to anyone who is motivated and wants to be part of that journey!
If you'd like to contribute to the project, refer to the [contributing documentation](https://docs.mindsdb.com/contribute/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

This project is released with a [Contributor Code of Conduct](https://github.com/mindsdb/mindsdb/blob/stable/CODE_OF_CONDUCT.md). By participating in this project, you agree to follow its terms.

Also, check out the [rewards and community programs](https://mindsdb.com/community?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).


### 💚 Current contributors <a name="Current contributors"></a>

<a href="https://github.com/mindsdb/mindsdb/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=mindsdb/mindsdb" />
</a>

Made with [contributors-img](https://contributors-img.web.app).

## 🔔 Subscribe to updates

Join our [Slack community](https://mindsdb.com/joincommunity) and subscribe to the monthly [Developer Newsletter](https://mindsdb.com/newsletter/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) to get product updates, information about MindsDB events and contests, and useful content, like tutorials.


## ⚖️ License <a name="License"></a>

For detailed licensing information, please refer to the [LICENSE file](https://github.com/mindsdb/mindsdb/blob/master/LICENSE)
