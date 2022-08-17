# SQL Alchemy and MindsDB

SQL Alchemy is a Python package, or a Python SQL toolkit, that provides object-relational mapping features for the Python programming language. It facilitates working with databases and Python. You can download it [here](https://www.sqlalchemy.org/) or run a `pip install sqlalchemy` command if you use a Linux system.

Please follow the instructions below to connect your MindsDB to SQL Alchemy.

=== "Connecting MindsDB Cloud to SQL Alchemy"

    You can use the Python code below to connect your Cloud MindsDB database to SQL Alchemy.

    ```python
    from sqlalchemy import create_engine

    user = 'MindsDB Cloud username' # replace this value
    password = 'MindsDB Cloud password' # replace this value
    host = 'cloud-mysql.mindsdb.com'
    port = 3306
    database = ''

    def get_connection():
            return create_engine(
                    url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database)
            )

    if __name__ == '__main__':
            try:
                    engine = get_connection()
                    print(f"Connection to the {host} for user {user} created successfully.")
            except Exception as ex:
                    print("Connection could not be made due to the following error: \n", ex)
    ```

    Please note that we use the following connection details:

    - Username is your MindsDB Cloud username
    - Password is your MindsDB Cloud password
    - Host is `cloud-mysql.mindsdb.com`
    - Port is `3306`
    - Database name is left empty

    To create a database connection, execute the code above. On success, the following output is expected:

    ```bash
    Connection to the cloud-mysql.mindsdb.com for user MindsDB-Cloud-Username created successfully.
    ```

=== "Connecting Local MindsDB to SQL Alchemy"

    You can use the Python code below to connect your local MindsDB database to SQL Alchemy.

    ```python
    from sqlalchemy import create_engine

    user = 'mindsdb'
    password = ''
    host = '127.0.0.1'
    port = 47334
    database = ''

    def get_connection():
            return create_engine(
                    url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database)
            )

    if __name__ == '__main__':
            try:
                    engine = get_connection()
                    print(f"Connection to the {host} for user {user} created successfully.")
            except Exception as ex:
                    print("Connection could not be made due to the following error: \n", ex)
    ```

    Please note that we use the following connection details:

    - Username is `mindsdb`
    - Password is left empty
    - Host is `127.0.0.1`
    - Port is `47334`
    - Database name is left empty

    To create a database connection, execute the code above. On success, the following output is expected:

    ```bash
    Connection to the 127.0.0.1 for user mindsdb created successfully.
    ```

!!! tip "What is next?"
    We recommend you to follow one of our tutorials or learn more about the [MindsDB Database](/sql/table-structure/).
