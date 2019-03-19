# MindsDB Basics

Here you will find a file [home_rentals.csv](https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv), containing the final rental pricing for some properties.

You can follow [this example on Google Colab](https://colab.research.google.com/gist/JohannesFerner/88773019cd385fe6ba0a9377a4779f40/mindsdb.ipynb)

### Goal
The goal is to be able to predict the best **rental_price** for a new properties given the information that we have in home_rentals.csv.


#### Learning


```python

from mindsdb import *

# First we initiate MindsDB
mdb = MindsDB()

# We tell mindsDB what we want to learn and from what data
mdb.learn(
    from_data="https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv", # the path to the file where we can learn from, (note: can be url)
    predict='rental_price', # the column we want to learn to predict given all the data in the file
    model_name='home_rentals' # the name of this model
)

```

**Note**: that the argument **from_data** can be a path to a file, a URL or a pandas data_frame


#### Predicting



```python

from mindsdb import *

# First we initiate MindsDB
mdb = MindsDB()

# use the model to make predictions
result = mdb.predict(predict='rental_price', when={'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190}, model_name='home_rentals')

# you can now print the results
print('The predicted price is ${price} with {conf} confidence'.format(price=result.predicted_values[0]['rental_price'], conf=result.predicted_values[0]['prediction_confidence']))

```

## Notes

#### About the Learning
The first thing we can do is to learn from the csv file. Learn in the scope of MindsDB is to let it figure out a neural network that can best learn from this data as well as train and test such model given the data that we have (learn more in [InsideMindsDB](../../InsideMindsDB.md)).

When you run this script, note that it will start printing something like:

```text
Test Error:0.002251171274110675, Accuracy:0.8817534675498809
[SAVING MODEL] Lowest ERROR so far! - Test Error: 0.002251171274110675 

```

The error is [RMSE](https://en.wikipedia.org/wiki/Root-mean-square_deviation) and the Accuracy is the [R squared](https://en.wikipedia.org/wiki/Coefficient_of_determination) value of the prediction.


#### About getting predictions from the model


Please note the **when** argument, in this case assuming we only know that:

* 'number_of_rooms': 2, 
* 'number_of_bathrooms':1 
* 'sqft': 1190

So long the columns that you pass in the when statement exist in the data it learnt from it will work (see columns in [home_rentals.csv](home_rentals.csv))



