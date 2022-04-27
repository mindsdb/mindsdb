# SELECT from files

The `SELECT from files` statement is used to select a file as a datasource. This allows the user to create a predictor from a file that has been uploaded to MindsDB.

## Example

This example will show how to upload a file to MindsDB Cloud and use it to create a predictor.

### Upload file to MindsDB Cloud

1. Login to MindsDB Cloud.
2. Navigate to  `Add Data ` located on the right navigation bar identified by a plug icon.
3. Click on the tab `Files` and the card `Import File`

![Add File](/assets/sql/add-file-data.png)


5. Name your file in `Table name`.
6. Click on `Save and Continue`.

![Model Status](/assets/sql/file.png)


### Select the file as datasource:

Query:
```sql
SELECT * FROM files.file_name;
```

Example:
```sql
SELECT * FROM files.home_rentals Limit 10;
```

![SELECT from file](/assets/sql/select_file.png)


### Create a predictor from file

Query:

```sql
CREATE PREDICTOR mindsdb.predictor_name from files (SELECT * from file_name) predict target_variable;
```
Example
```sql
CREATE PREDICTOR mindsdb.home_rentals_model from files (SELECT * from home_rentals) predict rental_price;
```
>Parameters:

> - predictor_name: chosen name for your predictor.

> - file_name: name of your file that's being used as a datasource.

> - target_variable: the column you want to predict.



