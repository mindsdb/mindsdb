---
id: jupyter-notebook
title: Running on Jupyter Notebook
---

## Create a notebook

Click on the Link [to Try it in your browser with Classic Notebook](https://mybinder.org/v2/gh/ipython/ipython-in-depth/master?filepath=binder/Index.ipynb) : 

You’ll see a screen load with ‘Binder’ at the top.  This should resolve to a screen, with a file menu near the top.

<blockquote class="imgur-embed-pub" lang="en" data-id="sYV91pv"><a href="//imgur.com/sYV91pv"></a></blockquote><script async src="//s.imgur.com/min/embed.js" charset="utf-8"></script>

On the far left to the file menu, select file, then drag down ‘New Notebook’ and from there select ‘Python 3’.

<blockquote class="imgur-embed-pub" lang="en" data-id="7m7huHY"><a href="//imgur.com/7m7huHY"></a></blockquote><script async src="//s.imgur.com/min/embed.js" charset="utf-8"></script>

You will then see Python command line

<blockquote class="imgur-embed-pub" lang="en" data-id="kl9kdv0"><a href="//imgur.com/kl9kdv0"></a></blockquote><script async src="//s.imgur.com/min/embed.js" charset="utf-8"></script>

## Installing mindsdb and running

In the command line type: `!pip install git+https://github.com/mindsdb/mindsdb.git@master --user --no-cache-dir --upgrade --force-reinstall;` then press the `Run` button in the top bar and wait for the install to finish.

<blockquote class="imgur-embed-pub" lang="en" data-id="MNkPyy3"><a href="//imgur.com/MNkPyy3"></a></blockquote><script async src="//s.imgur.com/min/embed.js" charset="utf-8"></script>

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
result = mdb.predict(when_data={'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190})

# The result will be an array containing predictions for each data point (in this case only one), a confidence for said prediction and a few other extra information
print('The predicted price is ${price} with {conf} confidence'.format(price=result[0]['rental_price'], conf=result[0]['rental_price_confidence']))
```
