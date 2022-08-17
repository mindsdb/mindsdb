# `#!sql SELECT files.[file]` Statement

## Description

The `#!sql SELECT from files.[file]` statement is used to select a `#!sql [file]` as a datasource. The main use is to create a predictor from a file that has been uploaded to MindsDB via the [MindsDB Editor](/connect/mindsdb_editor/).

!!! warning "Before using the `#!sql SELECT files.[file]`"
    Make sure to [upload the file via the MindsDB Editor](#upload-file-to-mindsdb-editor)

## Syntax

```sql
SELECT *
FROM files.[file_name];
```

On execution, we get:

```sql
+--------+--------+--------+--------+
| column | column | column | column |
+--------+--------+--------+--------+
| value  | value  | value  | value  |
+--------+--------+--------+--------+
```

Where:

| Name          | Description                                                                               |
| ------------- | ----------------------------------------------------------------------------------------- |
| `[file_name]` | Name of file uploaded to MindsDB via the [MindsDB SQL Editor](/connect/mindsdb_editor/)   |
| column        | Name of the column depending on the file uploaded                                         |
| value         | Value depending on the file uploaded                                                      |

## Example

This example shows how to use an uploaded file and create a predictor. But first, follow [this guide](https://docs.mindsdb.com/sql/create/file/) to upload a file to MindsDB Cloud.

### Select the file as datasource

```sql
SELECT *
FROM files.home_rentals
LIMIT 10;
```

On execution, we get:

```sql
+-----------------+---------------------+-------+----------+----------------+---------------+--------------+--------------+
| number_of_rooms | number_of_bathrooms | sqft  | location | days_on_market | initial_price | neighborhood | rental_price |
+-----------------+---------------------+-------+----------+----------------+---------------+--------------+--------------+
| 0               | 1                   | 484,8 | great    | 10             | 2271          | south_side   | 2271         |
| 1               | 1                   | 674   | good     | 1              | 2167          | downtown     | 2167         |
| 1               | 1                   | 554   | poor     | 19             | 1883          | westbrae     | 1883         |
| 0               | 1                   | 529   | great    | 3              | 2431          | south_side   | 2431         |
| 3               | 2                   | 1219  | great    | 3              | 5510          | south_side   | 5510         |
| 1               | 1                   | 398   | great    | 11             | 2272          | south_side   | 2272         |
| 3               | 2                   | 1190  | poor     | 58             | 4463          | westbrae     | 4123.812     |
| 1               | 1                   | 730   | good     | 0              | 2224          | downtown     | 2224         |
| 0               | 1                   | 298   | great    | 9              | 2104          | south_side   | 2104         |
| 2               | 1                   | 878   | great    | 8              | 3861          | south_side   | 3861         |
+-----------------+---------------------+-------+----------+----------------+---------------+--------------+--------------+
```

### Create a predictor from file

Query:

```sql
CREATE PREDICTOR mindsdb.[predictor_name]
FROM files 
    (SELECT * FROM [file_name])
PREDICT [target_variable];
```

Example:

```sql
CREATE PREDICTOR mindsdb.home_rentals_model
FROM files
    (SELECT * from home_rentals)
PREDICT rental_price;
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```
