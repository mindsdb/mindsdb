# Setup for Sourcecode via pip

This section describes how to deploy MindsDB from the source code. It is the preferred way to use MindsDB if you want to contribute to our code or debug MindsDB.

## Before You Start

There are some points that you should consider before jumping into the installation. Please have a look at them below.

### Pip and Python Versions

Due to some of our dependencies having issues with the latest versions of Python 3.9.x, we suggest using **Python 3.7.x or 3.8.x versions** for now. We are working on Python 3.9.x to be supported soon.

To successfully install MindsDB, use **Python 64-bit version**. Also, make sure that **Python >= 3.7** and **pip >= 19.3**. You can check the pip and python versions by running the `#!console pip --version` and `#!console python --version` commands.

Please note that depending on your environment and installed pip and python packages, you might have to use **pip3** instead of **pip** or **python3.x** instead of **py**. For example, `#!console pip3 install mindsdb` instead of `#!console pip install mindsdb`.

### How to Avoid Dependency Issues

Install MindsDB in a virtual environment using **pip** to avoid dependency issues.

### How to Avoid Common Errors

MindsDB requires around 3 GB of free disk space to install all of its dependencies. Make sure to allocate min. 3 GB of disk space to avoid the `IOError: [Errno 28] No space left on device while installing MindsDB` error.

Before anything, activate your virtual environment where your MindsDB is installed. It is to avoid the `No module named mindsdb` error.

The `ImportError: No module named {dependency name}` error may occur if you skip step 3 of the [Installation](#installation) section. Make sure you install all of the prerequisites.

If you encounter the `This site can’t be reached. 127.0.0.1 refused to connect.` error, please check the MindsDB server console to see if the server is still in the `starting` phase. But if the server has started and you still get this error, please report it on our [GitHub repository](https://github.com/mindsdb/mindsdb/issues).

## Installation

1. Clone the MindsDB repository:

    ```bash
    git clone git@github.com:mindsdb/mindsdb.git
    ```

2. Create a new virtual environment called `mindsdb-venv`:

    ```bash
    python -m venv mindsdb-venv
    ```

    Now, activate it:

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

5. Verify MindsDB installation by starting the MindsDB server:

    ```bash
    python -m mindsdb
    ```

6. Now, you can access the following:

    === "MindsDB APIs"
        ```
        http://127.0.0.1:47334/api
        ```

    === "MindsDB Studio"
        ```
        http://127.0.0.1:47334/
        ```

    === "MindsDB Studio using MySQL"
        ```bash
        mysql -h 127.0.0.1 --port 3306 -u mindsdb -p
        ```

## Further Issues?

You can try to replicate your issue using the [Docker setup](/setup/self-hosted/docker/).

Also, please create an issue with detailed description in the [MindsDB GitHub repository](https://github.com/mindsdb/mindsdb/issues) so we can help you. Usually, we review issues and respond within a few hours.

!!! tip "What's next?"
    We recommend you to follow one of our tutorials or learn more about the [MindsDB Database](/sql/table-structure/).
