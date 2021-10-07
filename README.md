<h1 align="center">
	<img width="300" src="https://github.com/mindsdb/mindsdb_native/blob/stable/assets/MindsDBColorPurp@3x.png?raw=true" alt="MindsDB">
	<br>

</h1>
<div align="center">
	<a href="https://github.com/mindsdb/mindsdb/actions"><img src="https://github.com/mindsdb/mindsdb/workflows/MindsDB%20workflow/badge.svg" alt="MindsDB workflow"></a>

  <a href="https://www.python.org/downloads/" target="_blank"><img src="https://img.shields.io/badge/python-3.6%20|%203.7|%203.8-brightgreen.svg" alt="Python supported"></a>
   <a href="https://pypi.org/project/MindsDB/" target="_blank"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>
  <a href="https://pypi.org/project/MindsDB/" target="_blank"><img src="https://img.shields.io/pypi/dm/mindsdb" alt="PyPi Downloads"></a>
  <a href="https://hub.docker.com/u/mindsdb" target="_blank"><img src="https://img.shields.io/docker/pulls/mindsdb/mindsdb" alt="Docker pulls"></a>
  <a href="https://www.mindsdb.com/"><img src="https://img.shields.io/website?url=https%3A%2F%2Fwww.mindsdb.com%2F" alt="MindsDB Website"></a>	
    <a href="https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ" target="_blank"><img src="https://img.shields.io/badge/slack-@mindsdbcommunity-brightgreen.svg?logo=slack " alt="MindsDB Community"></a>
	
  <h3 align="center">
    <a href="https://www.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Website</a>
    <span> | </span>
    <a href="https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Docs</a>
    <span> | </span>
    <a href="https://apidocs.mindsdb.com/">API Docs</a>
    <span> | </span>
    <a href="https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ">Community Slack</a>
    <span> | </span>
    <a href="https://github.com/mindsdb/mindsdb/issues?q=is%3Aopen+is%3Aissue+label%3Ahacktoberfest">Hacktoberfest</a>
  </h3>
  
</div>

[MindsDB](https://mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) is a predictive platform that makes databases intelligent and machine learning easy to use. It allows data analysts to build and visualize forecasts in BI dashboards without going through the complexity of ML pipelines, all through SQL. It also helps data scientists to streamline MLOps by providing advanced instruments for in-database machine learning and optimize ML workflows through a declarative JSON-AI syntax. [![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Machine%20Learning%20inside%20Databases%20&url=https://www.mindsdb.com&via=mindsdb&hashtags=ai,ml,machine_learning,neural_networks)

----------------------------------------

[Installation](https://github.com/mindsdb/mindsdb#installation) - [Overview](https://github.com/mindsdb/mindsdb#overview) - [Features](https://github.com/mindsdb/mindsdb#features) - [Database Integrations](https://github.com/mindsdb/mindsdb#database-integrations) - [Quickstart](https://github.com/mindsdb/mindsdb#quickstart) - [Documentation](https://github.com/mindsdb/mindsdb#documentation) - [Support](https://github.com/mindsdb/mindsdb#support) - [Contributing](https://github.com/mindsdb/mindsdb#contribution) - [Mailing lists](https://github.com/mindsdb/mindsdb#mailing-lists) - [License](https://github.com/mindsdb/mindsdb#license)

----------------------------

<h2 align="center">
   Machine Learning using SQL
   <br/>
   <br/>
  <img width="600" src="https://mindsdb-resources.s3.amazonaws.com/MindsDB+Glue.png" alt="MindsDB">	

</h2>

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

MindsDB automates and abstracts machine learning through virtual AI Tables in databases - all through SQL (MongoDB query syntax is also supported).

You can get forecasts from complex multivariate time-series data or detect anomalies in real-time without building & maintaining ETL pipelines, ML workflows and API integrations.

#### How it works:

1. Let MindsDB connect to your database.

2. Make MindsDB learn from historical data automatically by training a predictor using a single SQL statement. (if you'd rather configure some of your models manually or bring your own, MindsDB supports that too via declarative JSON-AI syntax). 

3. Make predictions immediately by querying MindsDB virtual AI Tables. There’s no need to deploy models.

4. Visualize forecasts in your BI dashboards, all through standard SQL. AI Tables behave the same way as normal database tables. 

Check our [docs](https://docs.mindsdb.com/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) and [blog](https://mindsdb.com/blog/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) for tutorials and use case examples.


## Features

* Automatic data pre-processing, feature engineering and encoding.
* Classification, regression, time-series tasks.
* Automatic model deployment through virtual AI Tables.
* Data quality check for potential biases & outliers.
* Model accuracy scoring and confidence intervals for each prediction.
* Batch predictions by joining predictor with other tables.
* Anomaly detection.
* Model explainability analysis with graphical user interface.
* GPU support for faster model training.
* Tune model internals with declarative JSON-AI syntax.
* Import your own models.
* HTTP API available

## Database Integrations

| Connect your Data |
|-|
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white" alt="Connect MongoDB"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/MySQL-00758F?style=for-the-badge&logo=mysql&logoColor=white" alt="Connect MySQL"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="Connect PostgreSQL"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/MariaDB-003545?style=for-the-badge&logo=mariadb&logoColor=white" alt="Connect MariaDB"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Apache Kafka-808080?style=for-the-badge&logo=apache-kafka&logoColor=white" alt="Connect MongoDB"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Snowflake-35aedd?style=for-the-badge&logo=snowflake&logoColor=blue" alt="Connect Snowflake"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Clickhouse-e6e600?style=for-the-badge&logo=clickhouse&logoColor=white" alt="Connect Clickhouse"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Cassandra-1287B1?style=for-the-badge&logo=apache%20cassandra&logoColor=white" alt="Connect Cassandra"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/redis-%23DD0031.svg?&style=for-the-badge&logo=redis&logoColor=white" alt="Connect Redis"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Microsoft%20SQL%20Sever-CC2927?style=for-the-badge&logo=microsoft%20sql%20server&logoColor=white" alt="Connect SQL Server"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Singlestore-5f07b4?style=for-the-badge&logo=singlestore&logoColor=white" alt="Connect Singlestore"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/ScyllaDB-53CADD?style=for-the-badge&logo=scylladbb&logoColor=white" alt="Connect ScyllaDB"></a> |
| <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/CockroachDB-426EDF?style=for-the-badge&logo=cockroachdb&logoColor=white" alt="Connect CockroachDB"></a> |

[:question: :wave: Missing integration?](https://github.com/mindsdb/mindsdb/issues/new?assignees=&labels=&template=feature-mindsdb-request.md)

## Quickstart

To get your hands on MindsDB, we recommend using the [Docker image](https://docs.mindsdb.com/deployment/docker/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) or simply sign up for a [free cloud account](https://cloud.mindsdb.com/signup?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).
Feel free to browse [documentation](https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) for other installation methods and tutorials.

## Documentation

You can find the complete documentation of MindsDB at [docs.mindsdb.com](https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).
Documentation for our HTTP API can be found at [apidocs.mindsdb.com](https://apidocs.mindsdb.com/).

## Support

If you found a bug, please submit an [issue on Github](https://github.com/mindsdb/mindsdb/issues).

To get community support, you can:
* Post at MindsDB [Slack community](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ).
* Ask for help at our [Github Discussions](https://github.com/mindsdb/mindsdb/discussions).
* Ask a question at [Stackoverflow](https://stackoverflow.com/questions/tagged/mindsdb) with a MindsDB tag.

If you need commercial support, please [contact](https://mindsdb.com/contact/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) the MindsDB team.

## Contributing

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
