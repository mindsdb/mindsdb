<h1 align="center">
	<img width="300" src="https://github.com/mindsdb/mindsdb_native/blob/stable/assets/MindsDBColorPurp@3x.png?raw=true" alt="MindsDB">
	<br>

</h1>
<p align="center">
	<a href="https://github.com/mindsdb/mindsdb/actions"><img src="https://github.com/mindsdb/mindsdb/workflows/MindsDB%20workflow/badge.svg" alt="MindsDB workflow"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.6%20|%203.7|%203.8-brightgreen.svg" alt="Python supported"></a>
   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>
  <a href="https://pypi.org/project/MindsDB/"><img src="https://img.shields.io/pypi/dm/mindsdb" alt="PyPi Downloads"></a>
  <a href="https://community.mindsdb.com/"><img src="https://img.shields.io/discourse/posts?server=https%3A%2F%2Fcommunity.mindsdb.com%2F" alt="MindsDB Community"></a>
  <a href="https://www.mindsdb.com/"><img src="https://img.shields.io/website?url=https%3A%2F%2Fwww.mindsdb.com%2F" alt="MindsDB Website"></a>	
</p>

MindsDB is an open-source AI layer for existing databases that allows you to effortlessly develop, train and deploy state-of-the-art machine learning models using SQL queries. [![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Machine%20Learning%20in%20one%20line%20of%20code%21&url=https://www.mindsdb.com&via=mindsdb&hashtags=ai,ml,machine_learning,neural_networks)

<h2 align="center">
   Predictive AI layer for existing databases
   <br/>
  <img width="600" src="https://mindsdbuploads.s3-us-west-2.amazonaws.com/ai-tables.gif" alt="MindsDB">	
</h2>


## Try it out

* [Installing MindsDB](https://docs.mindsdb.com/Installing/)
	* [Install on Linux](https://docs.mindsdb.com/installation/Linux/)
	* [Install on Windows](https://docs.mindsdb.com/installation/Windows/)
	* [Install on MacOS](https://docs.mindsdb.com/installation/MacOS/)
* [AI Tables](https://docs.mindsdb.com/databases/)
	* [AI Tables in MariaDB](https://docs.mindsdb.com/databases/MariaDB/)
	* [AI Tables in ClickHouse](https://docs.mindsdb.com/databases/Clickhouse/)
	* [AI Tables in MySQL](https://docs.mindsdb.com/databases/MySQL/)
	* [AI Tables in PostgreSQL](https://docs.mindsdb.com/databases/PostgreSQL/)
* [Learning from Examples](https://docs.mindsdb.com/tutorials/BasicExample/)
* [MindsDB Explainability GUI](https://docs.mindsdb.com/scout/Introduction/)
* [Frequently Asked Questions](https://docs.mindsdb.com/FAQ/)

### Installation


* **Desktop**:
  * **Virtual Environment**: We suggest you to run MindsDB on a virtual environment to avoid dependency issues. Make sure your Python version is >=3.6. To set up a virtual environment:
    1. Create and activate venv:
    ```bash
    python -m venv venv
    source venv/bin/activate
    ```
    2. Install MindsDB:
    ```bash
    pip install mindsdb
    ```
    3. Run MindsDB:
    ```bash
    python -m mindsdb
    ```
   >Note: Python 64 bit version is required. Depending on your environment, you might have to use `pip3` instead of `pip`, and `python3.x` instead of `python` in the above commands.*
 
  * **Host Environment**: You can use MindsDB on your own computer in under a minute, if you already have a python environment setup, just run the following command:
    1. Install MindsDB:
    ```bash
     pip install mindsdb --user
    ```
    2. Run MindsDB:
    ```bash
    python -m mindsdb
    ```

  >Note: Python 64 bit version is required. Depending on your environment, you might have to use `pip3` instead of `pip`, and `python3.x` instead of `python` in the above commands.*

  If for some reason this fail, don't worry, simply follow the [complete installation instructions](https://docs.mindsdb.com/Installing/) which will lead you through a more thorough procedure which should fix most issues.

* **Docker**: If you would like to run it all in a container simply:  

```bash
sh -c "$(curl -sSL https://raw.githubusercontent.com/mindsdb/mindsdb/stable/distributions/docker/build-docker.sh)"
```

## MindsDB AI tables demo using MariaDB (Video tutorial)

Please click on the image below to load the tutorial:

<div>
  <a href="https://www.youtube.com/embed/Tguat5jjD4g"><img src="https://img.youtube.com/vi/Tguat5jjD4g/0.jpg" alt="IMAGE ALT TEXT"></a>
</div>

## Contributing

To contribute to mindsdb, please check out our [Contribution guide](https://github.com/mindsdb/mindsdb/blob/stable/CONTRIBUTING.md).

### Current contributors

<a href="https://github.com/mindsdb/mindsdb/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=mindsdb/mindsdb" />
</a>

Made with [contributors-img](https://contributors-img.web.app).

## Report Issues

Please help us by [reporting any issues](https://github.com/mindsdb/mindsdb/issues/new/choose) you may have while using MindsDB.

## License

* [MindsDB License](https://github.com/mindsdb/mindsdb/blob/master/LICENSE)
