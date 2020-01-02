
![MindsDB](https://raw.githubusercontent.com/mindsdb/mindsdb/master/assets/logo_gh.png "MindsDB")
#

[![Build Status](https://travis-ci.org/mindsdb/mindsdb.svg?branch=master)](https://travis-ci.org/mindsdb/mindsdb)
[![PyPI version](https://badge.fury.io/py/MindsDB.svg)](https://badge.fury.io/py/MindsDB)
![PyPI - Downloads](https://img.shields.io/pypi/dm/mindsdb)

MindsDB's is an Explainable AutoML framework for developers.[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Machine%20Learning%20in%20one%20line%20of%20code%21&url=https://www.mindsdb.com&via=mindsdb&hashtags=ai,ml,machine_learning,neural_networks)


# MindsDB Native

With MindsDB you can build, train and use state of the art ML models in as simple as one line of code.
  
* [Installing MindsDB Native](https://mindsdb.github.io/mindsdb/docs/installing-mindsdb)
* [Learning from Examples](https://mindsdb.github.io/mindsdb/docs/basic-mindsdb)
* [MindsDB Explainability GUI](http://mindsdb.com/product) 
* [Frequently Asked Questions](https://mindsdb.github.io/mindsdb/docs/faq)
* [Provide Feedback to Improve MindsDB](https://mindsdb.typeform.com/to/c3CEtj)


## Try it out

### Installation



* **Desktop**: You can use MindsDB on your own computer in under a minute, if you already have a python environment setup, just run the following command, otherwise simply follow the [installation instructions Complete Manual](https://mindsdb.github.io/mindsdb/docs/installing-mindsdb).

```bash
 pip3 install mindsdb --user
```

* **Docker**: If you would like to run it all in a container simply:  

```bash
sh -c "$(curl -sSL https://raw.githubusercontent.com/mindsdb/mindsdb/master/distributions/docker/build-docker.sh)"
```


### Usage

Once you have MindsDB installed, you can use it as follows:

Import **MindsDB**:

```python

from mindsdb import Predictor

```

One line of code to **train a model**:

```python
# tell mindsDB what we want to learn and from what data
Predictor(name='home_rentals_price').learn(
    to_predict='rental_price', # the column we want to learn to predict given all the data in the file
    from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv" # the path to the file where we can learn from, (note: can be url)
)

```


One line of code to **use the model**:

```python

# use the model to make predictions
result = Predictor(name='home_rentals_price').predict(when={'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190})

# you can now print the results
print('The predicted price is ${price} with {conf} confidence'.format(price=result[0]['rental_price'], conf=result[0]['rental_price_confidence']))

```

Visit the documentation to [learn more](https://mindsdb.github.io/mindsdb/docs/basic-mindsdb)

* **Google Colab**: You can also try MindsDB straight here [![Google Colab](https://colab.research.google.com/assets/colab-badge.svg "MindsDB")](https://colab.research.google.com/drive/1qsIkMeAQFE-MOEANd1c6KMyT44OnycSb)
 

## Video Tutorial

Please click on the image below to load the tutorial:

[![here](https://img.youtube.com/vi/a49CvkoOdfY/0.jpg)](https://youtu.be/yr7fgqt9cfU)  

(Note: Please manually set it to 720p or greater to have the text appear clearly)

## MindsDB Graphical User Interface

You can also work with mindsdb via its graphical user interface ([download here](http://mindsdb.com/product)).
Please click on the image below to load the tutorial:

[![here](https://img.youtube.com/vi/fOwdv4j26CA/0.jpg)](https://youtu.be/fOwdv4j26CA)  


## MindsDB Lightwood: Machine Learning Lego Blocks

Under the hood of mindsdb there is lightwood, a Pytorch based framework that breaks down machine learning problems into smaller blocks that can be glued together seamleasly. More info about   [MindsDB lightwood's on GITHUB](https://github.com/mindsdb/lightwood/).

## Contributing

In order to make changes to mindsdb, the ideal approach is to fork the repository than clone the fork locally `PYTHONPATH`.

For example: `export PYTHONPATH=$PYTHONPATH:/home/my_username/mindsdb`.

To test if your changes are working you can try running the CI tests locally: `cd tests/ci_tests && python3 full_test.py`

Once you have specific changes you want to merge into master, feel free to make a PR.

## Report Issues

Please help us by reporting any issues you may have while using MindsDB.

https://github.com/mindsdb/mindsdb/issues/new/choose
