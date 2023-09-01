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

<p>
	<a href="https://github.com/mindsdb/mindsdb/actions"><img src="https://github.com/mindsdb/mindsdb/actions/workflows/release.yml/badge.svg" alt="MindsDB Release"></a>
	<a href="https://www.python.org/downloads/" target="_blank"><img src="https://img.shields.io/badge/python-3.8.x%7C%203.9.x-brightgreen.svg" alt="Python supported"></a>
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
	<a href="https://github.com/mindsdb/mindsdb/projects?type=classic">Contribute</a>
	<span> | </span>
	<a href="https://cloud.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo">Demo</a>
	<span> | </span>
	<a href="https://mindsdb.com/hackerminds-ai-app-challenge">Hackathon</a>
</h3>

</div>

----------------------------------------

[MindsDB's](https://mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) **AI Virtual Database** empowers developers to connect any AI/ML model to any datasource. This includes relational and non-relational databases, data warehouses and SaaS applications.
 [![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Build%20AI-Centered%20Applications%20&url=https://www.mindsdb.com&via=mindsdb&hashtags=ai,ml,nlp,machine_learning,neural_networks,databases,gpt3)

MindsDB offers three primary benefits to its users. 
1. Creating and managing AI models (LLM based Semantic Search and QnA, TimeSeries Forecasting, Anomaly Detection, Classification, Recommenders, etc) through an “enhanced SQL” abstraction. 
2. Automate training and finetuning AI models from data contained in any of the 130+ datasources we support.
3. Hook AI models to run automatically as new data is observed and plug the output into any of our integrations.

<img width="1089" alt="image" src="https://github.com/mindsdb/mindsdb/assets/5898506/5451fe7e-a854-4c53-b34b-769b6c7c9863">


[Installation](https://github.com/mindsdb/mindsdb#installation) - [Overview](https://github.com/mindsdb/mindsdb#overview) - [Features](https://github.com/mindsdb/mindsdb#features) - [Database Integrations](https://github.com/mindsdb/mindsdb#database-integrations) - [Quickstart](https://github.com/mindsdb/mindsdb#quickstart) - [Documentation](https://github.com/mindsdb/mindsdb#documentation) - [Support](https://github.com/mindsdb/mindsdb#support) - [Contributing](https://github.com/mindsdb/mindsdb#contributing) - [Mailing lists](https://github.com/mindsdb/mindsdb#mailing-lists) - [License](https://github.com/mindsdb/mindsdb#license)

----------------------------------------


## Demo

You can try MindsDB using our [demo environment](https://cloud.mindsdb.com/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) with sample data for the most popular use cases.

## Installation

The preferred way is to use MindsDB Cloud [free demo instance](https://cloud.mindsdb.com/home) or use a [dedicated instance](https://cloud.mindsdb.com/home). If you want to move to production, use [the AWS AMI image](https://aws.amazon.com/marketplace/seller-profile?id=03a65520-86ca-4ab8-a394-c11eb54573a9).

To install locally or on-premise, pull the latest Docker image:

```
docker pull mindsdb/mindsdb
```

## How it works

1. CONNECT MindsDB to your data platform. We support hundreds of integrations, and this list is constantly growing. If you can’t find the integration you need, please [let us know](https://mindsdb.com/joincommunity).
2. CREATE MODEL  and pick the AI Engine to learn from your data. The models get provisioned and deployed automatically and become ready for inference instantaneously.
    1. Pick pre-trained models like OpenAI’s GPT, Hugging Face, LangChain, etc, for NLP or generative AI use cases;
    2. or pick from a variety of state-of-the-art engines for classic machine Learning use cases (regression, classification, or time-series tasks);
    3. or [IMPORT](https://docs.mindsdb.com/custom-model/byom) custom model built with any ML framework to automatically deploy as [AI Tables](https://www.youtube.com/watch?v=tnB4Y9T1E2k).
3. Query models using [SELECT](https://docs.mindsdb.com/sql/api/select) statements, [API](https://docs.mindsdb.com/rest/usage) calls, or [JOIN](https://docs.mindsdb.com/sql/api/join) commands to make predictions for thousands or millions of data points simultaneously.
4.  Experiment with your models and [Fine-Tune](https://docs.mindsdb.com/sql/api/finetune) them to achieve the best results.
5. Automate your workflows with [JOBs](https://docs.mindsdb.com/sql/create/jobs). 

Follow the [quickstart guide](https://docs.mindsdb.com/quickstart?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) with sample data to get on-boarded as fast as possible.


## Data Integrations

MindsDB works with most SQL, NoSQL databases, data warehouses, and popular applications. You can find the list of all supported integrations [here](https://docs.mindsdb.com/data-integrations/all-data-integrations).


[:question: :wave: Missing integration?](https://github.com/mindsdb/mindsdb/issues/new?assignees=&labels=&template=feature-mindsdb-request.yaml)


## Documentation

You can find the complete documentation of MindsDB at [docs.mindsdb.com](https://docs.mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

## Support

If you found a bug, please submit an [issue on GitHub](https://github.com/mindsdb/mindsdb/issues/new/choose).

To get community support, you can:

* Post a question at MindsDB [Slack community](https://mindsdb.com/joincommunity).
* Ask for help at our [GitHub Discussions](https://github.com/mindsdb/mindsdb/discussions).
* Ask a question at [Stackoverflow](https://stackoverflow.com/questions/tagged/mindsdb) with a MindsDB tag.

If you need commercial support, please [contact](https://mindsdb.com/contact/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) MindsDB team.

## Contributing

A great place to start contributing to MindsDB is to check our GitHub projects :checkered_flag:

* Community contributor's [dashboard tasks](https://github.com/mindsdb/mindsdb/projects/8).
* [First timers only issues](https://github.com/mindsdb/mindsdb/issues?q=is%3Aissue+is%3Aopen+label%3Afirst-timers-only), if this is your first time contributing to an open source project.

We are always open to suggestions, so feel free to open new issues with your ideas, and we can guide you!

Being part of the core team is accessible to anyone who is motivated and wants to be part of that journey!
If you'd like to contribute to the project, refer to the [contributing documentation](https://docs.mindsdb.com/contribute/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).

This project is released with a [Contributor Code of Conduct](https://github.com/mindsdb/mindsdb/blob/stable/CODE_OF_CONDUCT.md). By participating in this project, you agree to follow its terms.

Also, check out the [rewards and community programs](https://mindsdb.com/community?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo).


### Current contributors

<a href="https://github.com/mindsdb/mindsdb/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=mindsdb/mindsdb" />
</a>

Made with [contributors-img](https://contributors-img.web.app).

## Subscribe to updates

Join our [Slack community](https://mindsdb.com/joincommunity) and subscribe to the monthly [Developer Newsletter](https://mindsdb.com/newsletter/?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) to get product updates, information about MindsDB events and contests, and useful content, like tutorials.


## License

MindsDB is licensed under [GNU General Public License v3.0](https://github.com/mindsdb/mindsdb/blob/master/LICENSE)
