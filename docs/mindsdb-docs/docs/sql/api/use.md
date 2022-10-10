# USE statement

The `use integration_name` statement provides an option to use the connected datasources and `#!sql SELECT` from the database tables. Even if you are connecting to MindsDB as MySQL database, you will still be able to preview or `#!sql SELECT` from your database. 
If you haven't created a datasource after connecting to your database check out the simple steps explained [here](/connect/#create-new-datasource).


## Preview the data

To connect to your database `use` the created datasource: 

```sql
use integration_name
```

Then, simply `#!sql SELECT` from the tables:

```sql
SELECT * FROM table_name;
```

![Use datasource](/assets/sql/use.png)