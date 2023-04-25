# Automate Financial Data Processing with MindsDB and Plaid

Are you tired of manually processing financial data? With MindsDB and Plaid, you can easily automate this tedious task.

First, connect your Plaid account by following these steps. Once you have your client ID, secret, and public key, create a database in MindsDB:

> Get client_id, secret and plaid_env from [here](https://dashboard.plaid.com/team/keys)
> and access_token can be generated with help of docs [here](https://plaid.com/docs/api/tokens/#itempublic_tokenexchange)

```sql
CREATE DATABASE my_plaid 
WITH 
    ENGINE = 'plaid',
    PARAMETERS = {
      "client_id": "YOUR_CLIENT_ID",
      "secret": "YOUR_SECRET",
      "access_token": "YOUR_PUBLIC_KEY",
      "plaid_env": "ENV"
    };
```


This creates a database called my_plaid. This database comes with a table called **transactions**  and **balance** which we can use to search for and analyze transactions


Analyzing transactions in SQL

Let's get a list of transactions for a specific account:


```sql
SELECT 
   id, merchant_name, authorized_date, amount ,payment_channel
FROM my_plaid.transactions 
WHERE 
   start_date = '2022-01-01' 
   AND end_date = '2023-04-11' 
LIMIT 20;
```

# Native Queries
Plaid integration also supports native queries, which allows you to call any function available in the Plaid API:


This will retrieve the latest transactions for the given account
```sql
SELECT * FROM my_plaid (
  get_transactions(
    start_date = '2022-01-01',
    end_date = '2022-02-01'
  )
);
```
# Building a Machine Learning Model

Now that we have our data, let's build a machine learning model that can predict future expenses. We will be using MindsDB's built-in regression engine to create this model.

```sql
CREATE MODEL mindsdb.expense_prediction
FROM my_plaid 
    ( SELECT  merchant_name, date, amount 
      FROM transactions 
      WHERE start_date='2023-01-01' 
      AND end_date='2023-04-11'; )
PREDICT amount
ORDER BY date
GROUP BY merchant_name
WINDOW 25
HORIZON 15
USING ENGINE = 'statsforecast';
```
This creates a virtual table called expense_prediction. We can use this table to make predictions on future transactions:


```sql
SELECT  expense_prediction.amount as predicted_amount
FROM mindsdb.expense_prediction
WHERE 
 merchant_name= 'UBER' 
 AND date ='2022-03-01';
```

```
+-----------------+
|predicted_amount |
+-----------------+
|       25.0      |
+-----------------+
```
# Schedule a Job

Finally, we can automate this process by scheduling a job that runs our machine learning model on new transactions every day. No more manual data processing!
