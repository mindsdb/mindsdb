### Connect to the Financial Modeling API
We start by creating a database to connect to the Financial Modeling API. 
Retrieve an api key from: 

~~~~sql
CREATE DATABASE my_table
WITH
  ENGINE = 'FINANCIAL_MODELING'
  PARAMETERS = {
    "api_key": <api_key>,
  };
~~~~

Now, you can use this established connection to query your database as follows:
~~~~sql
SELECT * FROM my_table;
~~~~

# git push origin Riyana/FinancialHandler
# cd mindsdb/integrations/handlers/financial_modeling_prep_handler
