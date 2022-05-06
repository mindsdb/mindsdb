# INSERT INTO Statement

The `#!sql INSERT INTO` statement is used to fill a table with the result of subselect. commonly used to persist predictions in the 

### Syntax: 

```sql
    INSERT INTO [integration_name].[table_name]
    [SELECT ...]
```

It performs a subselect ```#!sql [SELECT ...]``` and gets data from it there after it performs ```#!sql INSERT INTO TABLE [table_name]```  of integration [integration_name]

### Expected Output

* 


**Several examples:**

Here:

- int1 - name of integration where table is going to be created
- tbl1 - name of table to create
- tp3 - name of predictor
- int2.fish - table that involved in select. 
  - int2 - name of integration
  - fish - name of table


Insert into table tbl1 of integration int1 with result of query "select * from fish where date > '2015-12-31'" executed in integration int2 

```
    insert into int1.tbl1
    select * from int2.fish where date > '2015-12-31'
```

Insert into table tbl1 of integration int1 with result of prediction of predictor tp3.
```
    insert into int1.tbl1 (
        select * from int2.fish as ta  
        join mindsdb.tp3 as tb 
        where ta.date > '2015-12-31' 
    ) 
```

The same query as previous in different composition 
```
    insert into int1.tbl1 
    select * from (
        select * from int2.fish as ta                  
        where ta.date > '2015-12-31'
    )
    join mindsdb.tp3 as tb  
``` 
