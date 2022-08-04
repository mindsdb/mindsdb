
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
10. Click on `Test Connection...` to check if all the provided data allows you to connect to MindsDB

    ![Test](/assets/sql/test_connection_dbeaver.png)

11. Click on `OK`.

    ![Connect](/assets/sql/connectcloud.png)

!!! tip "Whitelist MindsDB Cloud IP address"
    If you need to whitelist MindsDB Cloud IP address to have access to your database, reach out to MindsDB team so we can share the Cloud static IP with you.

!!! tip "What is next?"
    We recommend you to follow one of our tutorials or learn more about the [MindsDB Database](/sql/table-structure/).
