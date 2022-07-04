# Build From Source Using pip

This section describes how to deploy MindsDB from the source code. It is the preferred way to use MindsDB if you want to contribute to our code or debug MindsDB.

???+ warning "Python 3.9"
    Currently, some of our dependencies have issues with the latest versions of Python 3.9.x. For now, our suggestion is to use **Python 3.7.x, or 3.8.x versions**.

???+ warning "Suggestions"
    Install MindsDB in a virtual environment when using **pip** to avoid dependency issues. Make sure your **Python>=3.7** and **pip>=19.3**.

## Installation

We recommend installing MindsDB inside a virtual environment to avoid dependency issues.

1. Clone the repository:

    ```bash
    git clone git@github.com:mindsdb/mindsdb.git
    ```

2. Create new virtual environment called mindsdb-venv:

    ```bash
    python -m venv mindsdb-venv
    ```

    And, activate it:

    ```bash
    source mindsdb-venv/bin/activate
    ```

3. Install MindsDB prerequisites:

    ```bash
    cd mindsdb && pip install -r requirements.txt
    ```

4. Install MindsDB:

    ```bash
    python setup.py develop
    ```

5. To verify everything works, start the MindsDB server:

    ```bash
    python -m mindsdb
    ```

6. Now you should be able to access:

    === "MindsDB APIs"
        ```
        http://127.0.0.1:47334/api
        ```

    === "MindsDB Studio"
        ```
        http://127.0.0.1:47334/
        ```

    === "MindsDB Studio Using mySQL"
        ```bash
        mysql -h 127.0.0.1 --port 3306 -u mindsdb -p
        ```

## Troubleshooting

### Common Issues

!!! failure "No module named mindsdb"
    If you get this error, make sure that your virtual environment is activated.

!!! failure "ImportError: No module named {dependency name}"
    This type of error can occur if you skipped the 3rd step. Make sure that you install all of the MindsDB requirements.

!!! failure "This site can’t be reached. 127.0.0.1 refused to connect."
    Please check the MindsDB server console in case the server is still in the `starting` phase. If the server has started and there is an error displayed, please report it on our [GitHub](https://github.com/mindsdb/mindsdb/issues)

### Still Having problems?

Don't worry! Try to replicate the issue using the official [docker setup](/setup/self-hosted/docker/), and please create an issue on our [Github repository](https://github.com/mindsdb/mindsdb/issues) as detailed as possible.

We'll review it and give you a response within a few hours.
