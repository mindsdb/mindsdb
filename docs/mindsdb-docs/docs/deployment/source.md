# Deploy from the source code

This section describes how to deploy MindsDB from the source code. This is the prefered way to use MindsDB if you want to contribute to our code or simply to debug MindsDB.

## Prerequisite

!!! warning "Python 3.9"
    Currently, some of our dependencies have issues with the latest versions of Python 3.9.x. For now, our suggestion is to use Python 3.7.x, or 3.8.x versions.

* [Python version](https://www.python.org/downloads/) >=3.6 (64 bit) and pip version >= 19.3.
* [Pip](https://pip.pypa.io/en/stable/installing/) (is usually pre-installed with the latest Python versions).
* [Git](https://git-scm.com/).

## Installation

We recommend installing MindsDB inside a virtual environment to avoid dependency issues.

1. Clone the repository:

    ```
    git clone git@github.com:mindsdb/mindsdb.git
    ```

2. Create a virtual environment and activate it:

    ```
    python3 -m venv mindsdb-venv
    source mindsdb-venv/bin/activate
    ```

3. Install MindsDB prerequisites:

    ```
    cd mindsdb && pip install -r requirements.txt
    ```

4. Install MindsDB:

    ```
    python setup.py develop
    ```

5. You're done!

To check if everything works, start the MindsDB server:

```
python -m mindsdb
```

* To access MindsDB APIs, visit `http://127.0.0.1:47334/api`.
* To access MindsDB Studio, visit  `http://127.0.0.1:47334/`


## Installation troubleshooting

!!! failure "No module named mindsdb"
    If you get this error, make sure that your virtual environment is activated.

!!! failure "ImportError: No module named {dependency name}"
    This type of error can occur if you skipped the 3rd step. Make sure that you install all of the MindsDB requirements.

!!! failure "This site can’t be reached. 127.0.0.1 refused to connect."
    Please check the MindsDB server console in case the server is still in the `starting` phase. If the server has started and there is an error displayed, please report it on our [GitHub](https://github.com/mindsdb/mindsdb/issues).

