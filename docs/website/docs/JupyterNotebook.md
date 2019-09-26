---
id: jupyter-notebook
title: Running on Jupyter Notebook
---

## Create a notbook

Go here - https://jupyter.org/

Click on the Link ‘Try it in your browser!’ https://jupyter.org/try

Click the top left tile ‘Try Jupyter with Python!’ https://mybinder.org/v2/gh/ipython/ipython-in-depth/master?filepath=binder/Index.ipynb

You’ll see a screen load with ‘Binder’ at the top.  This should resolve to a screen, with a file menu near the top.

![](https://imgur.com/sYV91pv)

On the far left to the file menu, select file, then drag down ‘New Notebook’ and from there select ‘Python 3’.

![](https://imgur.com/7m7huHY)

You will then see Python command line

![](https://imgur.com/kl9kdv0)

## Installing mindsdb and running

In the command line type: `!pip install git+https://github.com/mindsdb/mindsdb.git@master --user --no-cache-dir --upgrade --force-reinstall;` then press the `Run` button in the top bar and wait for the install to finish.

![](https://imgur.com/MNkPyy3)

Now we can run one of our mindsdb examples, first by training a model:

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

Then generating some predictions:

```python
mdb = mindsdb.Predictor(name='real_estate_model')

# use the model to make predictions
# Note: you can use the `when_data` argument if you want to use a file with one or more rows instead of a python dictionary
result = mdb.predict(when={'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190})

# The result will be an array containing predictions for each data point (in this case only one), a confidence for said prediction and a few other extra informations
print('The predicted price is ${price} with {conf} confidence'.format(price=result[0]['rental_price'], conf=result[0]['rental_price_confidence']))
```
