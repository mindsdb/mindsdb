This simple documentation should guide over how to bring ML capabilities to clickhouse. The idea is that you should be able to treat ML models just as if they were normal Clickhouse tables. What we have built so far allows you to create, train models, and finally query such models straight from the database. 


**How can you try it?

We are trying to make it as simple as possible, what we have now is 3 simple steps:

* First, install mindsdb which is a python based server that deals with the ML part.

```pip3 install mindsdb```

* Once installed you can tell mindsdb how to connect to your clickhouse database by modifying this file:
	
* Lastly, you simply run the server: 

```python3 -m mindsdb```

If everything worked you should be able to see a new database appear in your clickhouse server;

** Hands on ML  

For the sake of this example we will use a table pulled from a url that contains information about house rentals.

CREATE VIEW real_estate SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32')

Let’s create a model to predict the price for which we should rent a property.

You can simply insert into the mindsdb.predictors table to train new models

You should see that a new table was created

USE MINDSDB;
SHOW TABLES;

You should be able to make predictions as follows:



