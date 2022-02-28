# **SELECT from Files**

The SELECT from Files statement is to select a file as a datasource. This allows the user to create a predictor from a file that has been uploaded to MindsDB's Cloud/Database.

## Example

This example will show how to upload a file to the database via MindsDB Cloud and select data from a file to create a predictor.

### Upload file to MindsDB Cloud

1. Login to MindsDB Cloud.
2. Navigate to the 'Files' located on the right side bar.
3. Select 'File Upload' next to Your Files.
4. Select 'File' and choose your file which will be used as a datasource.
5. Name your file.
6. Select Upload.

Your file will be displayed in the list for your files.

![Upload file](https://raw.githubusercontent.com/chandrevdw31/mindsdb-tutorials/main/Assets/diabetes/upload_file.png)

### Connect to MindsDB using SQL client

You can connect to MindsDB SQL Server via [Local Deployment](https://docs.mindsdb.com/sql/connect/local/) or [MindsDB Cloud](https://docs.mindsdb.com/sql/connect/cloud/)

a. Open a terminal and run the following command:

```sql
mysql -h cloud.mindsdb.com --port 3306 -u cloudusername@mail.com -p
```
> The parameters required are:

> - -h: Host name of mindsdbs mysql api (by default takes cloud.mindsdb.com if not specified)
> - --port: TCP/IP port number for connection(3306)
> - -u: MindsDB Cloud username
> - -p: MindsDB Cloud password

b. Once connected, run the following command to connect to MindsDB's database:

```sql
use mindsdb;
```

### Create predictor from your file as a datasource

#### Select the file as your datasource:

Query:
```sql
SELECT * FROM files.file_name;
```

Example:
```sql
SELECT * FROM files.diabetes Limit 10;
```
>Parameters:
> - .file_name : The name of your file you are using as a datasource(as named when it was uploaded to the database).

Results:
```bash
mysql> select * from files.diabetes LIMIT 10;
+--------------------------+------------------------------+--------------------------+-----------------------------+----------------------+-----------------+----------------------------+------+----------+
| Number of times pregnant | Plasma glucose concentration | Diastolic blood pressure | Triceps skin fold thickness | 2-Hour serum insulin | Body mass index | Diabetes pedigree function | Age  | Class    |
+--------------------------+------------------------------+--------------------------+-----------------------------+----------------------+-----------------+----------------------------+------+----------+
| 6                        | 148                          | 72                       | 35                          | 0                    | 33.6            | 0.627                      | 50   | positive |
| 1                        | 85                           | 66                       | 29                          | 0                    | 26.6            | 0.351                      | 31   | negative |
| 8                        | 183                          | 64                       | 0                           | 0                    | 23.3            | 0.672                      | 32   | positive |
| 1                        | 89                           | 66                       | 23                          | 94                   | 28.1            | 0.167                      | 21   | negative |
| 0                        | 137                          | 40                       | 35                          | 168                  | 43.1            | 2.288                      | 33   | positive |
| 5                        | 116                          | 74                       | 0                           | 0                    | 25.6            | 0.201                      | 30   | negative |
| 3                        | 78                           | 50                       | 32                          | 88                   | 31.0            | 0.248                      | 26   | positive |
| 10                       | 115                          | 0                        | 0                           | 0                    | 35.3            | 0.134                      | 29   | negative |
| 2                        | 197                          | 70                       | 45                          | 543                  | 30.5            | 0.158                      | 53   | positive |
| 8                        | 125                          | 96                       | 0                           | 0                    | 0.0             | 0.232                      | 54   | positive |
+--------------------------+------------------------------+--------------------------+-----------------------------+----------------------+-----------------+----------------------------+------+----------+
10 rows in set (0.72 sec)
```

#### Create a predictor

Query:

```sql
CREATE PREDICTOR predictor_name from files (SELECT * from file_name) predict target_variable;
```
Example
```sql
CREATE PREDICTOR diabetic_data from files (SELECT * from diabetes) predict class;
```
>Parameters:

> - predictor_name: chosen name for your predictor.

> - file_name: name of your file that's being used as a datasource.

> - target_variable: the column you want to predict.

Results:
```bash
mysql> CREATE PREDICTOR diabetic_data from files (SELECT * from diabetes) predict class;
Query OK, 0 rows affected (5.16 sec)
```

#### Select your predictor

You can now select your predictor to check the training status with the following command:

Query:
```sql
SELECT * from mindsdb.predictors where name='predictor_name';
```
Example 
```sql
SELECT * FROM mindsdb.predictors where name='diabetic_data';
```
Results:
```bash
mysql> SELECT * FROM mindsdb.predictors where name='diabetic_data';
+---------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+------------------+
| name          | status   | accuracy           | predict | update_status | mindsdb_version | error | select_data_query | training_options |
+---------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+------------------+
| diabetic_data | complete | 0.6546310832025117 | Class   | up_to_date    | 22.2.2.1        | NULL  |                   |                  |
+---------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+------------------+
1 row in set (0.63 sec)
```
