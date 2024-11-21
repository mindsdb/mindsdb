
# Render

Renderer is using to convert AST-query to sql string using different sql dialects.

## How to use

```python
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender

renderer = SqlalchemyRender('mysql') # select dialect
sql = renderer.get_string(ast_query, with_failback=True)
```

If with_failback==True: in case if sqlalchemy unable to render query 
string will be returned from sql representation of AST-tree (with method to_string) 

## Parameterized queries

For getting query with parametes use `get_exec_params` function of sqlachemy render (as alternative to get_string)
It doesn't inject params to query but returned them separated
```python
query_str, params = renderer.get_exec_params(ast_query)
```
- query_str: insert into table values (%s, %s)
- params: [[1,2], [3,4]]

In handler this function could be used for bulk insert (for example executemany in postgres)


