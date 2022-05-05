To integrate your predictions into your DBT workflow, you will need to make four changes:

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