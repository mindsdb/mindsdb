# Connect to Snowflake Data Warehouse

Connecting MindsDB to the <a href="https://www.snowflake.com/" target="_blank">Snowflake</a> can be easily done with a few clicks.

#### Connect to Snowflake

1. From the left navigation menu, select Database Integration.
2. Click on the `ADD DATABASE` button.
3. In the `Connect to Database` modal window:
    1. Select Snowflake as the Supported Database.
    2. Add the Database name.
    3. Add the Hostname.
    4. Add Port.
    5. Add Account.
    6. Add Warehouse.
    7. Add Schema.
    8. Add protocol.
    9. Add User.
    10. Add Password for the user.
    11. Click on `CONNECT`.

![Connect to Snowflake](/assets/data/snowflake.gif)

#### Create new Datasource

1. Click on the `NEW DATASOURCE` button.
2. In the `Datasource from DB integration` modal window:
    1. Add Datasource Name.
    2. Add Database name.
    3. Add SELECT Query (e.g. SELECT * FROM my_database).
    4. Click on `CREATE`.

![Create Snowflake Datasource](/assets/data/snowflake-ds.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully connected to Snowflake from MindsDB Studio. The next step is to train the [Machine Learning model](/model/train).

