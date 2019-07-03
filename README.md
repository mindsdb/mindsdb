
![MindsDB](https://raw.githubusercontent.com/mindsdb/mindsdb/master/assets/logo_gh.png "MindsDB")
#

[![Build Status](https://travis-ci.org/mindsdb/mindsdb.svg?branch=master)](https://travis-ci.org/mindsdb/mindsdb)
[![PyPI version](https://badge.fury.io/py/MindsDB.svg)](https://badge.fury.io/py/MindsDB)

MindsDB's goal is to give developers easy access to the power of artificial neural networks for their projects.[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Machine%20Learning%20in%20one%20line%20of%20code%21&url=https://www.mindsdb.com&via=mindsdb&hashtags=ai,ml,machine_learning,neural_networks)

* [Installing MindsDB](https://mindsdb.github.io/mindsdb/docs/installing-mindsdb)
* [Learning from Examples](https://mindsdb.github.io/mindsdb/docs/basic-mindsdb)
* [Frequently Asked Questions](https://mindsdb.github.io/mindsdb/docs/faq)
* [Provide Feedback to Improve MindsDB](https://mindsdb.typeform.com/to/c3CEtj)


## Try it out

### Installation

You can use MindsDb on your own computer in under a minute, simply follow the [installation instructions](https://mindsdb.github.io/mindsdb/docs/installing-mindsdb) or, if you already have a python environment setup, just run:

```bash
 pip3 install mindsdb --user
```

You can try MindsDb on [Google Colab](https://colab.research.google.com/drive/1qsIkMeAQFE-MOEANd1c6KMyT44OnycSb)

You can also use the Docker image [here](https://github.com/mindsdb/mindsdb/tree/master/distributions/docker)

If you'd prefer to watch a video tutorial, you can find it [here](https://www.youtube.com/watch?v=a49CvkoOdfY). (Note: Please manually set it to 720p or greater to have the text appear clearly)

Having problems ? Please tell us about them with [an issue on github](https://github.com/mindsdb/mindsdb/issues/new/choose)

### Usage

Once you have MindsDB installed, you can use it as follows:

To **train a model**:

```python

from mindsdb import Predictor


# We tell mindsDB what we want to learn and from what data
Predictor(name='home_rentals_price').learn(
    to_predict='rental_price', # the column we want to learn to predict given all the data in the file
    from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv" # the path to the file where we can learn from, (note: can be url)
)

```


To **use the model**:

```python

from mindsdb import Predictor

# use the model to make predictions
result = Predictor(name='home_rentals_price').predict(when={'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190})

# you can now print the results
print('The predicted price is ${price} with {conf} confidence'.format(price=result[0]['rental_price'], conf=result[0]['rental_price_confidence']))

```

Visit the documentation to [learn more](https://mindsdb.github.io/mindsdb/docs/basic-mindsdb)

## Report Issues

Please help us by reporting any issues you may have while using MindsDB.

https://github.com/mindsdb/mindsdb/issues/new/choose
