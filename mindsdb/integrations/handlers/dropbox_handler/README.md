---
title: Dropbox
sidebarTitle: Dropbox
---

# Dropbox Handler

This documentation describes the integration of MindsDB with [Dropbox](https://www.dropbox.com/official-teams-page?_tk=paid_sem_goog_biz_b&_camp=1033325405&_kw=dropbox|e&_ad=708022104237||c&gad_source=1&gclid=EAIaIQobChMI3qGNp4WPiQMVMpeDBx0X3CdpEAAYASAAEgIb9PD_BwE), a storage service.

## Connection

Establish a connection to your Dropbox account from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE dropbox_datasource
WITH
    engine = 'dropbox',
    parameters = {
      "access_token": "ai.L-wqp3eP6r4cSWVklkKAdTNZ3VAuQjWuZMvIs1BzKvZNVW07rKbVNi5HbxvLc9q9D6qSfsf5VTsqYsNPGUkqSJBlpkr88gNboUNuhITmJG9mVw-Olniu4MO3BWVbEIphVxXxxxCd677Y"
    };
```

Required connection parameters include the following:

- `access_token`: The Dropbox access token that enables connection to your Dropbox app.

To get the `access_token`, go to the Dropbox App: https://www.dropbox.com/en_GB/developers.

## Usage

#### Execute the SQL statement

For fetching the files from Dropbox, you need to provide full paths. If you want to see
all the paths to your files in Dropbox, execute:

```sql
SELECT * FROM <your_integration_name>.files;
```

In this example, we will fetch the JSON file and display the data in MindsDB Studio.

```sql
SELECT * from dropbox_datasource.`/json_files/flower/iris.json`
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

The above examples utilize `dropbox_datasource` as the data source name defined in the `CREATE DATABASE` command.

## Troubleshooting Guide

<Warning>
`Database Connection Error`

- **Symptoms**: Failure to connect MindsDB with the Dropbox.
- **Checklist**:

1. Confirm that provided Dropbox credentials are correct. Try making a direct connection to the Dropbox using your local script with Dropbox Python SDK.
2. Ensure a stable network between MindsDB and Dropbox.
   </Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

- **Symptoms**: SQL queries failing or not recognizing object names containing spaces, special characters or prefixes.
- **Checklist**: 1. Ensure object names with spaces, special characters or prefixes are enclosed in backticks. 2. Examples:
  _ Incorrect: SELECT _ FROM integration.travel/travel*data.csv
  * Incorrect: SELECT _ FROM integration.'travel/travel_data.csv'
  _ Correct: SELECT \_ FROM integration.\`travel/travel_data.csv\`
  </Warning>
