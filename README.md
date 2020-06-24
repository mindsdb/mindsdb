<h1 align="center">
	<img width="300" src="https://github.com/mindsdb/mindsdb/blob/master/assets/MindsDBColorPurp@3x.png?raw=true" alt="MindsDB"> 
	<br>
	
</h1>

<p align="center">
    <a href="https://travis-ci.com/mindsdb/mindsdb"><img src="https://travis-ci.com/mindsdb/mindsdb.svg?branch=master" alt="Build status"></a>
   <a href="https://pypi.org/project/mindsdb-server/"><img src="https://badge.fury.io/py/mindsdb-server.svg" alt="PyPi Version"></a>
  <a href="https://pypi.org/project/mindsdb-server/"><img src="https://img.shields.io/pypi/dm/mindsdb-server" alt="PyPi Downloads"></a>
  <a href="https://community.mindsdb.com/"><img src="https://img.shields.io/discourse/posts?server=https%3A%2F%2Fcommunity.mindsdb.com%2F" alt="MindsDB Community"></a>
  <a href="https://apidocs.mindsdb.com/?version=latest"><img src="https://img.shields.io/badge/API-Documentation-green" alt="API Docs"></a>
</p>

# Mindsdb Server

This server uses flask_restx to define the API. Check [API's docs](https://apidocs.mindsdb.com/?version=latest) for detailed information about each endpoint.

## Installation

To install mindsdb server from PyPi run:
```
pip install mindsdb-server
```

## Usage
Once you have the server installed, you can start it by calling the `start_server()`:
```
import mindsdb as server

server.start_server()
```
Note that mindsdb server by default runs on 47334 port. To change that include port parameter:
```
server.start_server(port=43773)
```

## Development installation

To install and run mindsdb for development, activate your virtualenv and run:

```python
python setup.py develop
pip install -r requirements.txt
cd mindsdb
python3 server.py
```
You can provide basic parameters to the `server.py` call as:
* --port, the default is 47334
* --use_mindsdb_storage_dir, the default is False
* --host, the default is 0.0.0.0

Example: `python3 server.py --port 47333`

## The code inside mindsdb

 * ```namespaces/<endpoint.py>```: these contains a file per API resource 
 * ```namespaces/configs```: these are the configs for the API resources
 * ```namespaces/entitites```: these are specs for the document objects that can be returned by the API resources
 
