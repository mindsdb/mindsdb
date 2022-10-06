# `#!sql UPDATE FROM SELECT` Statement

## Description

The `#!sql UPDATE FROM SELECT` statement updates data in existing table. The data comes from a subselect query. 
It can be used as alternative to 'create table' and 'insert into' for store predictions in distinct columns of existing rows

## Syntax

Here is an example:

```sql
UPDATE 
    int2.table2 
SET
    predicted = source.result, 
FROM   
 (  
     SELECT p.result, p.prod_id, p.shop_id   
      FROM int1.table1 as t 
     JOIN mindsdb.pred1 as p
 ) AS source 
WHERE 
    prod_id = source.prod_id 
    and shop_id = source.shop_id 
```


And the steps followed by the syntax:

- It executes query from 'FROM' block to get the output dataset. 
In our example it is join of table *table1* (from integration *int1*) with predictor *pred1*.
It also can be select from integration
- *source* is the alias for fetched data
- then it updates *table2* from *int2* using conditions from `#!sql WHERE` block and fields for update from `#!sql SET` block
  - under the hood it splits input data to rows and executed this query for every row:
  ```sql
  UPDATE 
      table2 
  SET
      predicted = <row.result>, 
  WHERE 
      prod_id = <row.prod_id> 
      and shop_id = <row.shop_id>
  ```    

Note: in `#!sql WHERE` block it is better to use primary key for table
or set of rows that can be a primary key (and identifies every row in table). 
Otherwise, it can lead to unexpected results when one row in destination table was updated several times 
from different rows in source table (because conditions from different rows are fit).
