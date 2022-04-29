# Deploy Using pip on Linux

!!! warning "Python 3.9"
    Currently, some of our dependencies have issues with the latest versions of Python 3.9.x. For now, our suggestion is to use **Python 3.7.x, or 3.8.x versions**.

!!! warning "Suggestions"
    Install MindsDB in a virtual environment when using **pip** to avoid dependency issues. Make sure your **Python>=3.7** and **pip>=19.3**.

## Using the Python [`#!bash venv`](https://docs.python.org/3/library/venv.html) Module.

1. Create and activate venv:

    ```bash
    python -m venv mindsdb
    ```

    ```bash
    source mindsdb/bin/activate
    ```

2. Install MindsDB:

    ```bash
    pip install mindsdb
    ```

3. To verify that mindsdb was installed run:

    ```bash
    pip freeze
    ```

    You should see a list with the names of installed packages included but not limited to:

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

You will need [Anaconda](https://www.anaconda.com/products/individual) or [Conda](https://conda.io/projects/conda/en/latest/index.html)
installed and Python 64-bit version.

1. Open Anaconda Prompt and create new virtual environment 
    
    ```bash
    conda create -n mindsdb
    ```

    ```bash
    conda activate mindsdb
    ```

2. Install mindsdb in recently creeated virtual enviroment:

    ```bash
    pip install mindsdb
    ```


3. To verify that Mindsdb was installed run:

    ```bach 
    conda list
    ```

## Troubleshooting

If the installation fails, don't worry; follow the below instruction, which should fix most issues. If none of this works, try using the [docker setup](/deployment/docker/) and create an issue with the installation errors you got on our [Github repository](https://github.com/mindsdb/mindsdb/issues). We'll try to review it and give you a response within a few hours.

   
!!! failure "Installation fail"
    Note that **Python 64** bit version is required.

!!! failure "Installation fail"
    If you are using **Python 3.9** you may get installation errors. Some of the MindsDB's dependencies are not working with **Python 3.9**, so please downgrade to older versions for now. We are working on this and **Python 3.9** will be supported soon.

!!! failure "IOError: [Errno 28] No space left on device while installing MindsDB"
    MindsDB requires around 3GB of free disk space to install all of its dependencies.

!!! failure "No module named mindsdb"
    If you get this error, make sure that your **virtual environment**(where you installed mindsdb) is activated.
