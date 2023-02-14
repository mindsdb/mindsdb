<h1 align="center">
	<img width="300" src="https://github.com/mindsdb/mindsdb_native/blob/stable/assets/MindsDBColorPurp@3x.png?raw=true" alt="MindsDB">
	<br>

</h1>
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

<br>
	<a href="https://github.com/mindsdb/mindsdb/actions"><img src="https://github.com/mindsdb/mindsdb/workflows/MindsDB%20workflow/badge.svg" alt="MindsDB workflow"></a>

  <a href="https://www.python.org/downloads/" target="_blank"><img src="https://img.shields.io/badge/python-3.7.x%20|%203.8.x|%203.9.x-brightgreen.svg" alt="Python supported"></a>
   <a href="https://pypi.org/project/MindsDB/" target="_blank"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>
<img alt="PyPI - Downloads" src="https://img.shields.io/pypi/dm/Mindsdb">  <a href="https://hub.docker.com/u/mindsdb" target="_blank"><img src="https://img.shields.io/docker/pulls/mindsdb/mindsdb" alt="Docker pulls"></a>
  <a href="https://www.mindsdb.com/"><img src="https://img.shields.io/website?url=https%3A%2F%2Fwww.mindsdb.com%2F" alt="MindsDB Website"></a>	
    <a href="https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ" target="_blank"><img src="https://img.shields.io/badge/slack-@mindsdbcommunity-brightgreen.svg?logo=slack " alt="MindsDB Community"></a>
	</br>
	<a href="https://deepnote.com/project/Machine-Learning-With-SQL-8GDF7bc7SzKlhBLorqoIcw/%2Fmindsdb_demo.ipynb" target="_blank"><img src="https://deepnote.com/buttons/launch-in-deepnote-white.svg" alt="Launch in Deepnote"></a>
	</br>
	
  <h3 align="center">
    <a href="https://www.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Website</a>
    <span> | </span>
    <a href="https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Docs</a>
    <span> | </span>
    <a href="https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ">Community Slack</a>
    <span> | </span>
    <a href="https://github.com/mindsdb/mindsdb/projects">Contribute</a>
    <span> | </span>
    <a href="https://cloud.mindsdb.com">Demo</a>
    <span> | </span>
    <a href="https://mindsdb.com/in-database-nlp-tutorials-contest?utm_medium=community&utm_source=github&utm_campaign=22q4-huggingface-preview">NLP Contest</a>
  </h3>
  
</div>

----------------------------------------

[MindsDB](https://mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo)  ML-SQL Server enables machine learning workflows for the most powerful databases and data warehouses using SQL.  [![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Machine%20Learning%20inside%20Databases%20&url=https://www.mindsdb.com&via=mindsdb&hashtags=ai,ml,machine_learning,neural_networks,databases,sql)
* Developers can quickly add AI capabilities to your applications.
* Data Scientists can streamline MLOps by deploying ML models as AI Tables.
* Data Analysts can easily make forecasts on complex data (like multivariate time-series with high cardinality) and visualize them in BI tools like Tableau.

**NEW!** Check-out the [rewards and community programs.](https://mindsdb.com/community?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo)

----------------------------------------

[Installation](https://github.com/mindsdb/mindsdb#installation) - [Overview](https://github.com/mindsdb/mindsdb#overview) - [Features](https://github.com/mindsdb/mindsdb#features) - [Database Integrations](https://github.com/mindsdb/mindsdb#database-integrations) - [Quickstart](https://github.com/mindsdb/mindsdb#quickstart) - [Documentation](https://github.com/mindsdb/mindsdb#documentation) - [Support](https://github.com/mindsdb/mindsdb#support) - [Contributing](https://github.com/mindsdb/mindsdb#contributing) - [Mailing lists](https://github.com/mindsdb/mindsdb#mailing-lists) - [License](https://github.com/mindsdb/mindsdb#license)

----------------------------

<h2 align="center">
   Machine Learning using SQL 
   <br/>
   <br/>
  <img width="800" src="./docs/assets/mdb_image.png" alt="MindsDB">	

</h2>

<img width="1222" alt="image" src="https://user-images.githubusercontent.com/5898506/172740218-61db5eec-1bcf-4832-99d9-308709a426a7.png">

## Demo

You can try the Mindsdb ML SQL server here [(demo)](https://cloud.mindsdb.com).

## Installation

To install the latest version of MindsDB please pull the following Docker image:

```
docker pull mindsdb/mindsdb
```

Or, use PyPI:

```
pip install mindsdb
```

## Overview

MindsDB automates and abstracts machine learning models through virtual AI Tables:

Apart from abstracting ML models as AI Tables inside databases, MindsDB has a set of unique capabilities:

* Easily make predictions over very complex multivariate time-series data with high cardinality

* An open JSON-AI syntax to tune ML models and optimize ML pipelines in a declarative way


#### How it works:

1. Let MindsDB connect to your database.

2. Train a Predictor using a single SQL statement (make MindsDB learn from historical data automatically) or import your ML model to a Predictor via JSON-AI. 

3. Make predictions with SQL statements (Predictor is exposed as virtual AI Tables). There’s no need to deploy models since they are already part of the data layer.

Check our [docs](https://docs.mindsdb.com/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) and [blog](https://mindsdb.com/blog/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) for tutorials and use case examples.


## Features

* Automatic data pre-processing, feature engineering, and encoding
* Classification, regression, time-series tasks
* Bring models to production without “traditional deployment” as AI Tables
* Get models’ accuracy scoring and confidence intervals for each prediction
* Join ML models with existing data
* Anomaly detection
* Model explainability analysis 
* GPU support for models’ training
* Open JSON-AI syntax to build models and bring your ML blocks in a declarative way 


## Database Integrations

MindsDB works with most of the SQL and NoSQL databases and data Streams for real-time ML.

| Connect your Data | Connect your Data | Connect your Data
|-|-|-|
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Apache Kafka-808080?style=for-the-badge&logo=apache-kafka&logoColor=white" alt="Connect Apache Kafka"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Microsoft%20Access-A4373A?logo=microsoftaccess&logoColor=fff&style=for-the-badge" alt="Connect Microsoft Access"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Oracle-F80000?logo=oracle&logoColor=fff&style=for-the-badge" alt="Oracle Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Amazon%20Redshift-0466C8?style=for-the-badge&logo=amazon-aws&logoColor=white" alt="Connect Amazon Redshift"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Airtable-18BFFF?logo=airtable&logoColor=fff&style=for-the-badge" alt="Airtable Badge"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Pinot-000?logo=pinot&logoColor=fff&style=for-the-badge" alt="Pinot Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Cassandra-1287B1?style=for-the-badge&logo=apache%20cassandra&logoColor=white" alt="Connect Cassandra"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/DataStax-3A3A42?logo=datastax&logoColor=fff&style=for-the-badge" alt="DataStax Badge"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Amazon%20S3-569A31?logo=amazons3&logoColor=fff&style=for-the-badge" alt="Amazon S3 Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Clickhouse-e6e600?style=for-the-badge&logo=clickhouse&logoColor=white" alt="Connect Clickhouse"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Google Big Query-0466C8?logo=Big Query&logoColor=fff&style=for-the-badge" alt="Google Big Query Badge"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=fff&style=for-the-badge" alt="SQLite Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/CockroachDB-426EDF?style=for-the-badge&logo=cockroach-labs&logoColor=white" alt="Connect CockroachDB"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/ckan-7D929E?logo=ckan&logoColor=fff&style=for-the-badge" alt="ckan Badge"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Supabase-3ECF8E?logo=supabase&logoColor=fff&style=for-the-badge" alt="Supabase Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/MariaDB-003545?style=for-the-badge&logo=mariadb&logoColor=white" alt="Connect MariaDB"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Couchbase-EA2328?logo=couchbase&logoColor=fff&style=for-the-badge" alt="Couchbase Badge"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/TiDB-DD0031?logo=tidb&logoColor=fff&style=for-the-badge" alt="TiDB Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Microsoft%20SQL%20Sever-CC2927?style=for-the-badge&logo=microsoft%20sql%20server&logoColor=white" alt="Connect SQL Server"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/CrateDB-009DC7?logo=cratedb&logoColor=fff&style=for-the-badge" alt="CrateDB Badge"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Timescale-FDB515?logo=timescale&logoColor=fff&style=for-the-badge" alt="Timescale Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white" alt="Connect MongoDB"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/DoIt-00B388?logo=DoIt&logoColor=fff&style=for-the-badge" alt="DoIt Badge"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Amazon%20DynamoDB-4053D6?logo=amazondynamodb&logoColor=fff&style=for-the-badge" alt="Amazon DynamoDB Badge"></a> | 
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/MySQL-00758F?style=for-the-badge&logo=mysql&logoColor=white" alt="Connect MySQL"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=fff&style=for-the-badge" alt="Databricks Badge"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/YugabyteDB-02458D?logo=yugabytedb&logoColor=fff&style=for-the-badge" alt="YugabyteDB Badge"></a> 
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="Connect PostgreSQL"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/IBMDB2-008000?logo=IBMDB2&logoColor=fff&style=for-the-badge" alt="IBMDB2 Badge"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/openGauss-02458D?logo=opengauss&logoColor=fff&style=for-the-badge" alt="openGauss Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/QuestDB-d14671?style=for-the-badge&logo=questdb&logoColor=white" alt="Connect QuestDB"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Apache%20Druid-29F1FB?logo=apachedruid&logoColor=000&style=for-the-badge" alt="Apache Druid Badge"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/starrocks-000000?style=for-the-badge&logo=starrocks&logoColor=white" alt="StarRocks Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/redis-%23DD0031.svg?&style=for-the-badge&logo=redis&logoColor=white" alt="Connect Redis"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Vertica-000?logo=vertica&logoColor=fff&style=for-the-badge" alt="Vertica Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/SAP%20HANA-1661BE?style=for-the-badge&logo=sap&logoColor=white" alt="Connect SAP HANA"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Elastic-005571?logo=elastic&logoColor=fff&style=for-the-badge" alt="Elastic Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/ScyllaDB-53CADD?style=for-the-badge&logo=scylladbb&logoColor=white" alt="Connect ScyllaDB"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Firebird-F40D12?logo=Firebird&logoColor=fff&style=for-the-badge" alt="Firebird Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Singlestore-5f07b4?style=for-the-badge&logo=singlestore&logoColor=white" alt="Connect Singlestore"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Apache%20Hive-FDEE21?logo=apachehive&logoColor=000&style=for-the-badge" alt="Apache Hive Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Snowflake-35aedd?style=for-the-badge&logo=snowflake&logoColor=blue" alt="Connect Snowflake"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Informix-191A1B?logo=informix&logoColor=fff&style=for-the-badge" alt="Informix Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Teradata-e36c42?style=for-the-badge&logo=teradata&logoColor=white" alt="Connect Teradata"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Matrixone-02458D?logo=Matrixone&logoColor=fff&style=for-the-badge" alt="Matrixone Badge"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Trino-dd00a1?style=for-the-badge&logo=trino&logoColor=white" alt="Connect Trino"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Monetdb-0099E5?logo=Monetdb&logoColor=fff&style=for-the-badge" alt="Monetdb Badge"></a> |


[:question: :wave: Missing integration?](https://github.com/mindsdb/mindsdb/issues/new?assignees=&labels=&template=feature-mindsdb-request.yaml)

## Quickstart

To get your hands on MindsDB, we recommend using the [Docker image](https://docs.mindsdb.com/setup/self-hosted/docker/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) or simply sign up for a [free cloud account](https://cloud.mindsdb.com/signup?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).
Feel free to browse [documentation](https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) for other installation methods and tutorials.

## Documentation

You can find the complete documentation of MindsDB at [docs.mindsdb.com](https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

## Support

If you found a bug, please submit an [issue on GitHub](https://github.com/mindsdb/mindsdb/issues/new/choose).

To get community support, you can:
* Post at MindsDB [Slack community](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ).
* Ask for help at our [GitHub Discussions](https://github.com/mindsdb/mindsdb/discussions).
* Ask a question at [Stackoverflow](https://stackoverflow.com/questions/tagged/mindsdb) with a MindsDB tag.

If you need commercial support, please [contact](https://mindsdb.com/contact/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) MindsDB team.

## Contributing

A great place to start contributing to MindsDB will be our GitHub projects for checkered_flag:
* Community writers [dashboard tasks](https://github.com/mindsdb/mindsdb/projects/7).
* Community code contributors [dashboard tasks](https://github.com/mindsdb/mindsdb/projects/8).

Also, we are always open to suggestions so feel free to open new issues with your ideas and we can guide you!

Being part of the core team is accessible to anyone who is motivated and wants to be part of that journey!
If you'd like to contribute to the project, refer to the [contributing documentation](https://docs.mindsdb.com/contribute/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

Please note that this project is released with a [Contributor Code of Conduct](https://github.com/mindsdb/mindsdb/blob/stable/CODE_OF_CONDUCT.md). By participating in this project, you agree to abide by its terms.

### Current contributors

<a href="https://github.com/mindsdb/mindsdb/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=mindsdb/mindsdb" />
</a>

Made with [contributors-img](https://contributors-img.web.app).

## Mailing lists

Subscribe to MindsDB Monthly [Community Newsletter](https://mindsdb.com/newsletter/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) to get general announcements, release notes, information about MindsDB events, and the latest blog posts.
You may also join our [beta-users](https://mindsdb.com/beta-tester/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) group, and get access to new beta features.


## License

MindsDB is licensed under [GNU General Public License v3.0](https://github.com/mindsdb/mindsdb/blob/master/LICENSE)
