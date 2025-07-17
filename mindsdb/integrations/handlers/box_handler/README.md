---
title: Box
sidebarTitle: Box
---

# Box Handler

This documentation describes the integration of MindsDB with [Box](https://www.box.com), a storage service.

## Connection

Establish a connection to your Box account from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE box_datasource
WITH
    engine = 'box',
    parameters = {
      "token": ""
    };
```

Required connection parameters include the following:

- `token`: The Box developer token that enables connection to your Box app.

To get the `token`, go to the Box Developer Console: https://app.box.com/developers/console

## Usage

#### Execute the SQL statement

For fetching the files from Box, you need to provide full paths. If you want to see
all the paths to your files in Box, execute:

```sql
SELECT * FROM <your_integration_name>.files;
```

In this example, we will fetch the JSON file and display the data in MindsDB Studio.

```sql
SELECT * from box_datasource.`/json_files/flower/iris.json`
```

#### Output

| sepalLength | sepalWidth | petalLength | petalWidth | species |
| ----------- | ---------- | ----------- | ---------- | ------- |
| 5.1         | 3.5        | 1.4         | 0.2        | setosa  |
| 4.9         | 3          | 1.4         | 0.2        | setosa  |
| 4.7         | 3.2        | 1.3         | 0.2        | setosa  |
| 4.6         | 3.1        | 1.5         | 0.2        | setosa  |
| 5           | 3.6        | 1.4         | 0.2        | setosa  |

Wrap the file in backticks (\`) to avoid issues parsing the provided SQL statements. This is especially important when the file contains spaces, special characters or prefixes, such as `my-folder/my-file.csv`.
Currently, the supported file formats are CSV, TSV, JSON, and Parquet.

The above examples utilize `box_datasource` as the data source name defined in the `CREATE DATABASE` command.

## Troubleshooting Guide

<Warning>
`Database Connection Error`

- **Symptoms**: Failure to connect MindsDB with the Box.
- **Checklist**:

1. Confirm that provided Box credentials are correct. Try making a direct connection to the Box using your local script with Box Python SDK.
2. Ensure a stable network between MindsDB and Box.
   </Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

- **Symptoms**: SQL queries failing or not recognizing object names containing spaces, special characters or prefixes.
- **Checklist**: 1. Ensure object names with spaces, special characters or prefixes are enclosed in backticks. 2. Examples:
  _ Incorrect: SELECT _ FROM integration.travel/travel*data.csv
  * Incorrect: SELECT _ FROM integration.'travel/travel_data.csv'
  _ Correct: SELECT \_ FROM integration.\`travel/travel_data.csv\`
  </Warning>
