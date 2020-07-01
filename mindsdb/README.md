This simple documentation should guide over how to bring ML capabilities to clickhouse. The idea is that you should be able to treat ML models just as if they were normal Clickhouse tables. What we have built so far allows you to create, train models, and finally query such models straight from the database. 


## How can you try it?

We are trying to make it as simple as possible, what we have now is 3 simple steps:

1. Install mindsdb which is a python based server that deals with the ML part: `pip3 install mindsdb`

2. Then you simply run the server:  `python3 -m mindsdb`

3. [Optional] When you run mindsdb you should see it creates a config file (e.g. `/home/your_user/mindsdb/etc/config.json`), edit the `default_clickhouse` key of this file to specify how to connect to your clickhouse instance (if it runs on localhost:8123 with the default user and no password this step is not needed)

If everything worked you should be able to see a new database called `mindsdb` appear in your clickhouse server.

## Hands on ML  

For the sake of this example we will use a table pulled from a url that contains information about house rentals.

```
CREATE TABLE default.home_rentals (number_of_rooms String, number_of_bathrooms String, sqft Int64, location String, days_on_market Int64, initial_price Int64, neighborhood String, rental_price Float64)  ENGINE=URL('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/benchmarks/home_rentals/dataset/train.csv', CSVWithNames)
```

First, we'll train a predictor that predict a home's rental price based on the other columns in the table:

```
INSERT INTO mindsdb.predictors (name, predict_cols, select_data_query) VALUES('rentals_predictor','rental_price','SELECT * FROM default.home_rentals WHERE days_on_market <= 60');
```

You should see mindsdb output some training logs, we'll have to wait a few minutes unitl the predictor is fully trained.

Once that's done you can give some input data and get a prediction like this:

```
SELECT rental_price FROM mindsdb.rentals_predictor WHERE initial_price=900 and number_of_rooms='2';
```

Or, you can select some other data from a similar table (or the same table) and get a bunch of predictions.

```
SELECT rental_price FROM mindsdb.rentals_predictor WHERE `select_data_query`='SELECT * FROM default.home_rentals WHERE days_on_market > 60';
```


There's also some explainability features, confidence ranges for numerical predictions and confidence values for prediction but we are still tinkering around how to expose those in clickhouse.

There's also a GUI with which you can visualize your predictors and the data it was trained on, plus a few extra insights about both. You can download it from here: https://www.mindsdb.com/product


