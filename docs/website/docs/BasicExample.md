---
id: basic-mindsdb
title: Learning from Examples
---

These are basic examples of mindsdb usage in predicting real estate prices for an area.


## Goal
The goal is to be able to predict the best **rental_price** for a new properties given the information that we have in home_rentals.csv.

### Learning

```python
import mindsdb

# Instantiate a mindsdb Predictor
mdb = mindsdb.Predictor(name='real_estate_model')

# We tell the Predictor what column or key we want to learn and from what data
mdb.learn(
    from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv", # the path to the file where we can learn from, (note: can be url)
    to_predict='rental_price', # the column we want to learn to predict given all the data in the file
)
```

**Note**: that the argument **from_data** can be a path to a json, csv (or other separators), excel given as a file or as a URL, or a pandas Dataframe

### Predicting

```python
mdb = mindsdb.Predictor(name='real_estate_model')

# use the model to make predictions
# Note: you can use the `when_data` argument if you want to use a file with one or more rows instead of a python dictionary
result = mdb.predict(when={'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190})

# The result will be an array containing predictions for each data point (in this case only one), a confidence for said prediction and a few other extra informations
print('The predicted price is ${price} with {conf} confidence'.format(price=result[0]['rental_price'], conf=result[0]['rental_price_confidence']))
```


## Notes

### About the Learning
The first thing we can do is to learn from the csv file. Learn in the scope of MindsDB is to let it figure out a neural network that can best learn from this data as well as train and test such model given the data that we have (learn more in [InsideMindsDB](/mindsdb/docs/inside-mindsdb)).

When you run this script, note that it will start logging various information about the data and about the training process.

This information can be useful in allowing you to figure out which parts of your data are of low quality or might contain erroneous values

### About getting predictions from the model

Please note the **when** argument, in this case assuming we only know that:

* 'number_of_rooms': 2,
* 'number_of_bathrooms':1
* 'sqft': 1190

So long the columns that you pass in the when statement exist in the data it learned from it will work (see columns in [home_rentals.csv](https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv))

### Running online

You can follow [this example](https://colab.research.google.com/drive/1qsIkMeAQFE-MOEANd1c6KMyT44OnycSb)  on Google Colab
