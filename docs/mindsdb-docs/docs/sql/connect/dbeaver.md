
# Connect from DBeaver

1. From the navigation menu, click `Connect to database`
2. Search `MySQL 8+`

    ![Connect mysql 8](/assets/sql/dbeaver8.png)

3. Select the `MySQL 8+` or `MySQL`
4. Click on `Next`
5. Add the Hostname:
    * cloud.mindsdb.com - for MindsDB cloud
    * 127.0.0.1 - for local deployment
6. Add the Database name (leave empty)
7. Add Port 
    * 3306 - for MindsDB cloud
    * 47335 - for local deployment
8. Add the database user
    * MindsDB Cloud username
    * mindsdb - for local deployment
9. Add Password for the user 
    * MindsDB Cloud password
    * empty - for local deployment
10. Click on `OK`.

    ![Connect](/assets/sql/connectcloud.png)

## MindsDB Database

At startup mindsdb database will contain 3 tables `predictors`, `commands` and `databases`. 

![Connect](/assets/sql/show.png)

All of the newly trained machine learning models will be visible as a new record inside the `predictors` table. The `predictors` columns contains information about each model as:

* name - The name of the model.
* status - Training status(training, complete, error).
* predict - The name of the target variable column.
* accuracy - The model accuracy.
* update_status - Trainig update status(up_to_date, updating).
* mindsdb_version - The mindsdb version used.
* error - Error message info in case of an errror.
* select_data_query - SQL select query to create the datasource.
* training options - Additional training parameters.


!!! tip "Whitelist MindsDB Cloud IP address"
    If you need to whitelist MindsDB Cloud IP address to have access to your database, reach out to MindsDB team so we can share the Cloud static IP with you.
