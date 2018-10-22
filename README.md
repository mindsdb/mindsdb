
# MindsDB

MindsDB's goal is to make it very simple for developers to use the power of artificial neural networks in their projects. 


* [Installing MindsDB](docs/Installing.md)
* [Config Settings](docs/Config.md)
* [Learning from Examples](docs/examples/basic/README.md)
* [Inside MindsDB](docs/InsideMindsDB.md)



## Quick Overview

It's very simple to setup [(learn more)](docs/Installing.md)

```bash
 pip3 install mindsdb --user
```

Once you have MindsDB installed, you can use it as follows [(learn more)](docs/examples/basic/README.md):


To **train a model**:



```python

from mindsdb import *

# First we initiate MindsDB
mdb = MindsDB()

# We tell mindsDB what we want to learn and from what data
mdb.learn(
    from_data="https://raw.githubusercontent.com/mindsdb/main/master/docs/examples/basic/home_rentals.csv", # the path to the file where we can learn from, (note: can be url)
    predict='rented_price', # the column we want to learn to predict given all the data in the file
    model_name='home_rentals' # the name of this model
)

```


To **use the model**:


```python

from mindsdb import *

# First we initiate MindsDB
mdb = MindsDB()

# use the model to make predictions
result = mdb.predict(predict='rented_price', when={'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190}, model_name='home_rentals')

# you can now print the results
print('The predicted price is ${price} with {conf} confidence'.format(price=result.predicted_values[0]['rented_price'], conf=result.predicted_values[0]['prediction_confidence']))

```
