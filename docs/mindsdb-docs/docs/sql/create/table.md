CREATE TABLE
============

The CREATE TABLE statement is used to create table and fill it with result of subselect. 

The syntax is:
```
    create [or replace] table integration_name.table_name
    [(]  <select expression> [)]
```

This command:
- performs subselect and gets data from it. 
- Then it creates table <table_name> in integration <integration_name>. 
    - If 'replace' is indicated then table in integration will be droped beforehand. 
    - if no 'replace' keyword is set and table already exists there will be error.
- performs insert into created table.

**Several examples:**

Here:

- int1 - name of integration where table is going to be created
- tbl1 - name of table to create
- tp3 - name of predictor
- int2.fish - table that involved in select. 
  - int2 - name of integration
  - fish - name of table
 

Create table tbl1 of integration int1 with result of query "select * from fish where date > '2015-12-31'" executed in integration int2 
```
    create or replace table int1.tbl1
    select * from int2.fish where date > '2015-12-31'
```
  
Create table tbl1 of integration int1 with result of prediction of predictor tp3. 
```
    create table int1.tbl1 (
        select * from int2.fish as ta  
        join mindsdb.tp3 as tb 
        where ta.date > '2015-12-31' 
    )    
```

The same query as previous in different composition 
```
    create table int1.tbl1 
    select * from (
        select * from int2.fish as ta                  
        where ta.date > '2015-12-31'
    )
    join mindsdb.tp3 as tb 
```



