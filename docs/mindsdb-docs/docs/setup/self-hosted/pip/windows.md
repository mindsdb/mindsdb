# Setup for Windows via pip

## Before You Start

There are some points that you should consider before jumping into the installation. Please have a look at them below.

### Pip and Python Versions

Due to some of our dependencies having issues with the latest versions of Python 3.9.x, we suggest using **Python 3.7.x or 3.8.x versions** for now. We are working on Python 3.9.x to be supported soon.

To successfully install MindsDB, use **Python 64-bit version**. Also, make sure that **Python >= 3.7** and **pip >= 19.3**. You can check the pip and python versions by running the `#!console pip --version` and `#!console python --version` commands.

Please note that depending on your environment and installed pip and python packages, you might have to use **pip3** instead of **pip** or **python3.x** instead of **py**. For example, `#!console pip3 install mindsdb` instead of `#!console pip install mindsdb`.

### How to Avoid Dependency Issues

Install MindsDB in a virtual environment using **pip** to avoid dependency issues.

Or you could try to install MindsDB with [Anaconda](https://www.anaconda.com/products/individual) and run the installation from the **Anaconda prompt**.

### Installing torch or torchvision

If the installation fails when installing **torch** or **torchvision**, try to install them manually by following the instructions on their [official website](https://pytorch.org/get-started/locally/).

## Using the Python [`#!bash venv`](https://docs.python.org/3/library/venv.html) Module

1. Create a new virtual environment called `mindsdb`:

    ```console
    py -m venv mindsdb
    ```

    Now, activate it:

    ```console
    .\mindsdb\Scripts\activate.bat
    ```

2. Install MindsDB:

    ```console
    pip install mindsdb
    ```

3. Verify MindsDB installation:

    ```console
    pip freeze
    ```

    You should see a list of installed packages including but not limited to the following:

    ```bash
    ...
    alembic==1.7.7
    aniso8601==9.0.1
    appdirs==1.4.4
    lightgbm==3.3.0
    lightwood==22.4.1.0
    MindsDB==22.4.5.0
    mindsdb-datasources==1.8.2
    mindsdb-sql==0.3.3
    mindsdb-streams==0.0.5
    ...
    ```

## Using Anaconda

Here, you need either [Anaconda](https://www.anaconda.com/products/individual) or [Conda](https://conda.io/projects/conda/en/latest/index.html)
installed on your machine.

1. Open Anaconda prompt and create a new virtual environment 

    ```console
    conda create -n mindsdb
    ```

    Now, activate it:

    ```console
    conda activate mindsdb
    ```

2. Install MindsDB:

    ```console
    pip install mindsdb
    ```

3. Verify MindsDB installation:

    ```console
    conda list
    ```

    You should see a list of installed packages including but not limited to the following:

    ```bash
    ...
    alembic==1.7.7
    aniso8601==9.0.1
    appdirs==1.4.4
    lightgbm==3.3.0
    lightwood==22.4.1.0
    MindsDB==22.4.5.0
    mindsdb-datasources==1.8.2
    mindsdb-sql==0.3.3
    mindsdb-streams==0.0.5
    ...
    ```

## Further Issues?

You can try to replicate your issue using the [Docker setup](/setup/self-hosted/docker/).

Also, please create an issue with detailed description in the [MindsDB GitHub repository](https://github.com/mindsdb/mindsdb/issues) so we can help you. Usually, we review issues and respond within a few hours.
