# `#!sql SELECT files.[file]` Statement

## Description

The `#!sql SELECT from files.[file]` statement is used to select a `#!sql [file]` as a datasource. The main use is to,, create a predictor from a file that has been uploaded to MindsDB.

!!! warning "Before using the `#!sql SELECT files.[file]`"
    Make sure to upload the [file] via the MindsDB Editor

## Syntax

```sql
SELECT * FROM files.[file_name];
```

On execution:

```sql
+--------+--------+--------+--------+
| column | column | column | column |
+--------+--------+--------+--------+
| value  | value  | value  | value  |
+--------+--------+--------+--------+
```

Where:

|               | Description                                       |
| ------------- | ------------------------------------------------- |
| `[file_name]` | Name of file uploaded to mindsDB                  |
| column        | Name of the column depending on the file uploaded |
| value         | Value depending on the file uploaded              |

## Example

This example will show how to upload a file to MindsDB Cloud and use it to create a predictor.

### Upload file to MindsDB Editor

1. Connect to the MindsDB Editor 
2. Navigate to `Add Data` located on the right navigation bar identified by a plug icon.
3. Click on the tab `Files` and the card `Import File`
    <figure markdown> 
        ![Add File](/assets/sql/add-file-data.png){ width="800", loading=lazy  }
        <figcaption></figcaption>
    </figure>
4. Name your file in `Table name`.
5. Click on `Save and Continue`.

<figure markdown> 
    ![Upload Status](/assets/sql/file.png){ width="800", loading=lazy  }
    <figcaption></figcaption>
</figure>


### Select the file as datasource

```sql
SELECT * FROM files.home_rentals Limit 10;
```

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

Example

```sql
CREATE PREDICTOR mindsdb.home_rentals_model
FROM files
    (SELECT * from home_rentals)
PREDICT rental_price;
```
