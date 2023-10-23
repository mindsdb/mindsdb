# Test the Microsoft Access data integration

This README provides instructions for testing the Microsoft Access data integration in MindsDB.

Test Suites:
- MindsDB installed via Docker
- MS Access (Office Professional 2021)
- Ubuntu 20.04.6

For more details, refer to the related [GitHub Issue](https://github.com/mindsdb/mindsdb/issues/3947) and the [Microsoft Access documentation](https://docs.mindsdb.com/integrations/data-integrations/microsoft-access) in the MindsDB documentation.

## Test Cases for Microsoft Access


-----
### 1. Create a Microsoft Access Datasource on MindsDB 

**Description:**
To use this handler and connect to the Microsoft Access in MindsDB.

**Screenshot Result: Query Failed: Can't connect to db: Handler 'access' can not be used**

![Test 1](https://github.com/adeyinkaezra123/mindsdb/assets/65364356/28fa7b52-c570-420c-947b-a8c2bd89e1a5)

Possible Cause can be found [here](https://github.com/mindsdb/mindsdb/issues/3947)

-----

**Further tests cannot be executed due to the exception raised above**
