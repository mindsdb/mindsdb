## CKAN Integration handler

This handler facilitates integration with [CKAN](https://ckan.org/).
an open-source data catalog platform for managing and publishing open data. CKAN organizes datasets and stores data in its [DataStore](http://docs.ckan.org/en/2.11/maintaining/datastore.html).To retrieve data from CKAN, the [CKAAPI](https://github.com/ckan/ckanapi) must be used.

## Installation

To use the CKAN handler, you need to have MindsDB installed. If you haven't installed MindsDB yet, you can do so using pip:

```bash
pip install mindsdb
```

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

After setting up the connection, you can query CKAN data using SQL. The CKAN handler provides three main tables:

- `package_ids`: Lists all packages (datasets) in the CKAN instance.
- `resource_ids`: Lists all resources across all packages.
- `datastore`:  Allows querying individual datastore resources.

Here's an example of how to query a CKAN dataset:

```sql
SELECT * FROM datastore WHERE resource_id = 'your-resource-id';
```

## Limitations

- The handler currently supports read operations only. Write operations are not supported.
- The handler currently supports read operations only. Write operations are not supported.
- Performance may vary depending on the size of the CKAN instance and the complexity of your queries.
- The handler may not work with all CKAN instances, especially those with custom configurations.
- The handler does not support all CKAN API features. Some advanced features may not be available.


