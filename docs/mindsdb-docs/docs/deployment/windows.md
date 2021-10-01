
# Deploy using Anaconda

!!! warning "Python 3.9"
    Currently, some of our dependencies have issues with the latest versions of Python 3.9.x. For now, our suggestion is to use Python 3.7.x, or 3.8.x versions.

You will need <a href="https://www.anaconda.com/products/individual" target="_blank">Anaconda</a> or <a href="https://conda.io/projects/conda/en/latest/index.html" target="_blank">Conda</a> installed and Python 64bit version. Then open Anaconda Prompt and:

1. Create new virtual environment and install mindsdb:

    ```
    conda create -n mindsdb
    ```

    ```
    conda activate mindsdb
    ```

    ```
    pip install mindsdb
    ```

2. To verify that mindsdb was installed run:

    ```
    conda list
    ```

You should see a list with the names of installed packages.

# Deploy using pip

!!! warning "Python 3.9"
    Currently, some of our dependencies have issues with the latest versions of Python 3.9.x. For now, our suggestion is to use Python 3.6.x, 3.7.x, or 3.8.x versions.

We suggest you to install MindsDB in a virtual environment when using `pip` to avoid dependency issues. Make sure your Python version is **>=3.6** and pip version **>= 19.3**.

1. Create new virtual environment called mindsdb:

    ```
    py -m venv mindsdb
    ```

    And, activate it:

    ```
    .\mindsdb\Scripts\activate.bat
    ```

2. Install MindsDB:

    ```
    pip install mindsdb
    ```

3. To verify that mindsdb was installed run:

    ```
    pip freeze
    ```
    
You should see a list with the names of installed packages.


## Troubleshooting

If the installation fails, don't worry, simply follow the below bellow instruction which should fix most issues. If none of this works, try using the [docker container]() and create an issue with the installation errors you got on our [Github repository](https://github.com/mindsdb/mindsdb/issues). We'll try to review the issue and give you response within a few hours.


!!! failure "Installation fail"
    Note that **Python 64** bit version is required. 

!!! failure "pip command not found fail"
    Depending on your environment, you might have to use **pip3** instead of **pip**, and **python3.x** instead of **py** in the above commands e.g

    ```
    pip3 install mindsdb
    ```

!!! failure "Installation fail"
    If you are using **Python 3.9** you may get installation errors. Some of the MindsDB's dependencies are not working with **Python 3.9**, so please downgrade to older versions for now. We are working on this and **Python 3.9** will be supported soon.

!!! failure "Installation fail"
    If installation fails when installing **torch** or **torchvision** try manually installing them following the simple instructions on their [official website](https://pytorch.org/get-started/locally/).

!!! failure "Installation fails because of system dependencies"
    Try installing MindsDB with [Anaconda](https://www.anaconda.com/products/individual), and run the installation from the **anaconda prompt**.