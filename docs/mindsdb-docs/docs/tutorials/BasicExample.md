---
id: basic-mindsdb
title: Learning from Examples
---

This is a basic example of mindsdb_native usage in predicting the real estate prices for an area. If you want to follow out visually, watch below video:

<iframe width="560" height="315" src="https://www.youtube.com/embed/W69iSFgNpgQ" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

## Goal
The goal is to be able to predict the best **rental_price** for new properties given the information that we have in home_rentals.csv.

### Learning

```python
from mindsdb_native import Predictor

# We tell the Predictor what column or key we want to learn and from what data
Predictor(name='home_rentals_price').learn(
    from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv", # the path to the file where we can learn from, (note: can be url)
    to_predict='rental_price', # the column we want to learn to predict given all the data in the file
)
```

**Note**: that the argument **from_data** can be a path to a json, csv (or other separators), excel given as a file or as a URL, or a pandas Dataframe

### Predicting

```python
# use the model to make predictions
result = Predictor(name='home_rentals_price').predict(when_data={'number_of_rooms': 1, 'initial_price': 1222, 'sqft': 1190})

# The result will be an array containing predictions for each data point (in this case only one), a confidence for said prediction and a few other extra information
print('The predicted price is between ${price} with {conf} confidence'.format(price=result[0].explanation['rental_price']['confidence_interval'], conf=result[0].explanation['rental_price']['confidence']))
```

## Notes

### About the Learning
The first thing we can do is to learn from the csv file. Learn in the scope of MindsDB is to let it figure out a neural network that can best learn from this data as well as train and test such a model given the data that we have.

<iframe width="560" height="315" src="https://www.youtube.com/embed/b4nWvf9o2ls" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

When you run this script, note that it will start logging various information about the data and about the training process.

This information can be useful in allowing you to figure out which parts of your data are of low quality or might contain erroneous values.

### About getting predictions from the model

Please note the **when_data** argument, in this case assuming we only know that:

* 'number_of_rooms': 1,
* 'initial_price':1222
* 'sqft': 1190

So, as long as the columns that you pass in the **when_data** statement exists in the data it learned from it will work (see columns in [home_rentals.csv](https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv)).

### Running online

You can follow [this example](https://colab.research.google.com/drive/1qsIkMeAQFE-MOEANd1c6KMyT44OnycSb) on Google Colab.
