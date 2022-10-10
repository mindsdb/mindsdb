# MySQL Handler Tests

Tests pull mindsdb/mysql-handler-test docker image and launch a container
with predefined database


## To run tests locally:

```
python test_mysql_handler.py  # from the current directory

python mindsdb/integrations/handlers/mysql_handler/tests/test_mysql_handler.py # from the root of the repository
```

## Adjust the test docker image

You may find a configuration of the docker image [here](/docker)
