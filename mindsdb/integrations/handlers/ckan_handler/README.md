## CKAN Integration handler

This handler facilitates integration with [CKAN](https://ckan.org/).
an open-source data catalog platform for managing and publishing open data. CKAN organizes datasets and stores data in its [DataStore](http://docs.ckan.org/en/2.11/maintaining/datastore.html).To retrieve data from CKAN, the [CKAAPI](https://github.com/ckan/ckanapi) must be used.

# Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect SAP HANA to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

The CKAN handler is included with MindsDB by default, so no additional installation is required.

## Configuration

To use the CKAN handler, you need to provide the URL of the CKAN instance you want to connect to. You can do this by setting the `CKAN_URL` environment variable. For example:

```sql
CREATE DATABASE ckan_datasource
WITH ENGINE = 'ckan',
PARAMETERS = {
    "url": "https://your-ckan-instance-url.com",
    "api_key": "your-api-key-if-required"
};
```

> **_NOTE:_** Some CKAN instances will require you to provide an API Token. You can create one in the CKAN user panel.

## Usage

The CKAN handler provides three main tables:

- `datasets`: Lists all datasets in the CKAN instance.
- `resources`: Lists all resources metadata across all packages.
- `datastore`:  Allows querying individual datastore resources.

## Example Queries

1. List all datasets:

    ```sql
    SELECT * FROM `your-datasource`.datasets;
    ```

2. List all resources:

    ```sql
    SELECT * FROM `your-datasource`.resources ;
    ```

3. Query a specific datastore resource:

    ```sql
    SELECT * FROM `your-datasource`.datastore WHERE resource_id = 'your-resource-id';
    ```

Replace `your-resource-id-here` with the actual resource ID you want to query.

## Querying Large Resources

The CKAN handler supports automatic pagination when querying datastore resources. This allows you to retrieve large datasets without worrying about API limits.

You can still use the `LIMIT` clause to limit the number of rows returned by the query. For example:

```sql
SELECT * FROM ckan_datasource.datastore 
WHERE resource_id = 'your-resource-id-here' 
LIMIT 1000;
```

## Limitations

- The handler currently supports read operations only. Write operations are not supported.
- Performance may vary depending on the size of the CKAN instance and the complexity of your queries.
- The handler may not work with all CKAN instances, especially those with custom configurations.
- The handler does not support all CKAN API features. Some advanced features may not be available.
- The datastore search will return limited records up to 32000. Please refer to the [CKAN API](https://docs.ckan.org/en/2.11/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_search_sql) documentation for more information.
