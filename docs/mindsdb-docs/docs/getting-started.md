# Getting Started

MindsDB integrates with the most popular databases and also with the DBT and MLflow workflow you already have.

<sup><sub>To try MindsDB right away, without bringing your own data or model, check out our [Quick Start Guide](/).</sub></sup>

1. Choose your MindsDB installation path.

    === "MindsDB Cloud"

        !!! example ""
            Create your [free MindsDB Cloud account](https://cloud.mindsdb.com/signup).

            ![Cloud Signup](/assets/cloud-signup.png)

    === "Docker"

        !!! example ""
            To get started with a Docker installation, begin with our [Docker instructions](/deployment/docker).

1. Open your SQL client and connect to MindsDB.

    <sup><sub>If you do not already have a preferred SQL client, we recommend [DBeaver Community Edition](https://dbeaver.io/download/)</sub></sup>

    === "MindsDB Cloud"

        !!! example ""
            1. Create a new MySQL connection.

                ![DBeaver Create Connection](/assets/dbeaver-create-connection.png)
            
            1. Configure it using the following parameters, as well as the username and password you created above:

                ```
                    Host: mysql.mindsdb.com
                    Port: 3306
                    Database: mindsdb
                ```

                ![DBeaver Configure Connection](/assets/dbeaver-configure-cloud-connection.png)

    === "Docker"

        !!! example ""
            1. Create a new MySQL connection.

                ![DBeaver Create Connection](/assets/dbeaver-create-connection.png)
            
            1. Configure it using the following parameters.  Password remains empty.

                ```
                    Host: localhost
                    Port: 47335
                    Database: mindsdb
                    Username: mindsdb
                ```

                ![DBeaver Configure Connection](/assets/dbeaver-configure-docker-connection.png)

1. Connect your data to MindsDB using the [`CREATE DATABASE` syntax](https://docs.mindsdb.com/sql/create/databases/).

    <sup><sub>Example taken from our [Quick Start Guide](/quickstart/#connect-your-data).</sub></sup>

    ![DBeaver Create Database](/assets/dbeaver-create-database.png)

1. You can now preview the available data with a standard `SELECT`.

    <sup><sub>Example taken from our [Quick Start Guide](/quickstart/#preview-your-data).</sub></sup>

    ![DBeaver Preview Data](/assets/dbeaver-preview-data.png)

1. Now you are ready to create your model, using the [`CREATE PREDICTOR` syntax](https://docs.mindsdb.com/sql/create/predictor/).  If you already have a model in MLFlow, you can connect to your model as well.

    === "MindsDB is creating my model"

        !!! example ""
            Example taken from our [Quick Start Guide](/quickstart/#connect-your-data).

            ![DBeaver Create Predictor](/assets/dbeaver-create-predictor-simple.png)

    === "My model is in MLflow"

        !!! example ""
            Example taken from our [Docker Guide](/deployment/docker).

            <div id="create-predictor">
              <style>
                #create-predictor code { background-color: #353535; color: #f5f5f5 }
              </style>
            ```
            mysql> CREATE PREDICTOR mindsdb.home_rentals_predictor
                -> FROM example_data (select * from demo_data.home_rentals)
                -> PREDICT rental_price
                -> USING url.predict='http://host.docker.internal:1234/invocations',
                -> format='mlflow',
                -> dtype_dict={"alcohol": "integer", "chlorides": "integer", "citric acid": "integer", "density": "integer", "fixed acidity": "integer", "free sulfur dioxide": "integer", "pH": "integer", "residual sugar": "integer", "sulphates": "integer", "total sulfur dioxide": "integer", "volatile acidity": "integer"};
            Query OK, 0 rows affected (0.21 sec)
            ```
            </div>

1. The [`SELECT` syntax](/sql/api/select) will allow you to make a prediction based on features.

    <sup><sub>Example taken from our [Quick Start Guide](/quickstart/#preview-your-data).</sub></sup>

    ![DBeaver Home Rentals Prediction](/assets/dbeaver-home-rentals-prediction.png)
    ![DBeaver Home Rentals Prediction Results](/assets/dbeaver-home-rentals-prediction-results.png)

1. To integrate your predictions into your DBT workflow, you will need to make four changes:

    === "profiles.yml"

        ```
        mindsdb:
            type: mysql
            host: mysql.mindsdb.com
            user: mindsdb.user@example.com
            password: mindsdbpassword
            port: 3306
            dbname: mindsdb
            schema: example_data
            threads: 1
            keepalives_idle: 0 # default 0, indicating the system default
            connect_timeout: 10 # default 10 seconds
        ```

    === "schema.yml"

        ```
        version: 2

        models:
              - name: predicted_rentals
                description: "Integrating MindsDB predictions and historical   data"
        ```

    === "predicted_rentals.sql"

        ```
        with predictions as (
            SELECT hrp.rental_price as predicted_price, hr.rental_price as actual_price
            FROM mindsdb.home_rentals_predictor hrp
            JOIN exampleData.demo_data.home_rentals hr
            WHERE hr.number_of_bathrooms=2 AND hr.sqft=1000;
        )
        select * from predictions;
        ```

    === "dbt_project.yml"

        ```
        models:
            home_rentals:
                +materialized: view
        ```
