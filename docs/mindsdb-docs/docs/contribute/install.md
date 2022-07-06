# Installing MindsDB for Development

Before you contribute to MindsDB you will need to install it. There are a few options to do that by installing from source, PyPi or using our Docker image. Our preferred method for development is source install.


### Installing MindsDB from the source

Before installing MindsDB make sure you have `Git` and `Python 3.7.x or 3.8.x` versions installed.

1. Fork MindsDB Repository from [GitHub](https://github.com/mindsdb/mindsdb/fork).
2. Clone the fork locally `git@github.com:YOUR_USERNAME/mindsdb.git`.
3. Create a new virtual environment as `python3 -m venv mindsdbenv`.
4. Activate the virtual environment `source mindsdbenv/bin/activate`.
5. Install dependencies `cd mindsdb & pip install -e .`
6. Start MindsDB `python3 -m mindsdb`.

If everything works, in the console you should see a similar message:

```
...
2022-06-28 16:21:46,942 - INFO -  - GUI available at http://127.0.0.1:47334/
2022-06-28 16:21:47,010 - INFO - Starting MindsDB Mysql proxy server on tcp://127.0.0.1:47335
2022-06-28 16:21:47,015 - INFO - Waiting for incoming connections...
mysql API: started on 47335
http API: started on 47334
```