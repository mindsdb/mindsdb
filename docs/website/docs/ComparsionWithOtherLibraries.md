---
id: comparison-mindsdb
title: Comparison with other Libraries
---

How's MindsDB compared to other libraries, we will compare some of the libraries and show what differentiates Mindsdb from the rest.

## Models

With libraries such as Tensorflow, Sklearn, Pytorch you must have the expertise to build models from sratch and test them for accuracy.

With Mindsdb anyone can build state of the art models, you do not need to know how to build models.

## Data Preprocessing

Building models require most of the time for cleaning the data, normalizing the data, converting it into dataframes or tensors and making sure that you have checked for null values, categorical values.

Mindsdb can read data from csv, json, excel, url, dataframe or even a MySql table, tell it which colum(s) it should predict. It will automatically process the data for you and give insights about the data.

## Prediction

Getting predictions from the model will require the data on which you want to predict, for getting accurate predictions the models should be trained well. The model should not overfit or underfit the training data.

Getting predictions with Mindsdb is more like writing **queries** than to code the whole prediction system.

## Code Samples

Here we are going to use **[home_rentals.csv](https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv)** dataset for comparison purpose. Our goal is to predict the rental_price of the house given the information we have in home_rental.csv

### Preprocessing

When working with data we first have to see what type of data we are dealing with, in our case we have some numerical, categorical data. We first need to convert categorical data columns into numerical data.

```python
# loading data
import pandas as pd
data = pd.read_csv("home_rentals .csv")

# dealing with categorical values
data=pd.get_dummies(data, prefix=['condition','type'], columns=['location','neighborhood'])

# splitting data into train and test set
from sklearn.model_selection import train_test_split
train_data, test_data = train_test_split(data, test_size=0.2)

# seperating the input and output
train_label = train_data['rental_price']
test_label = test_data['rental_price']
del train_data['rental_price']
del test_data['rental_price']
```

Now we will check which type of model should be built. We will be going with simple linear regression.

### Tensorflow

```python
# placeholders for input data and label
X = tf.placeholder('float')
Y = tf.placeholder('float')

W = tf.Variable(tf.random.normal(), name = "weight")
b = tf.Variable(tf.random.normal(), name = "bias")

learning_rate = #your learning rate
epochs = # no of times data should be fed

y_pred = tf.add(tf.multiply(X, W), b)
cost = tf.reduce_sum(tf.pow(y_pred-Y, 2)) / (2 * len(train_data))
# Choosing the optimizer
optimizer = tf.train.GradientDescentOptimizer(learning_rate).minimize(cost)
# Initialize the Global variables
init = tf.global_variables_initializer()

# Run inside a session
with tf.Session() as sess:
    sess.run(init)
    for epoch in range(epochs):
        # feeding the training data
        for (_x, _y) in zip(train_data, train_label):
            sess.run(optimizer, feed_dict = {X : _x, Y : _y})
        # Calculate the cost
        c = sess.run(cost, feed_dict = {X : train_data, Y : train_label})
        print("epoch", (epoch + 1), ": cost =", c, "W =", sess.run(W), "b =", sess.run(b))
    # save your weights and bias
    weight = sess.run(W)
    bias = sess.run(b)

# Getting the predictions
predictions = weight* (test_data) + bias
print(predictions)
```

### Sklearn

```python
import sklearn
from sklearn.linear_model import LinearRegression

regressor = LinearRegression()

# feed the training data and label to train the model
regressor.fit(train_data, train_label)

# get predictions for the test data
y_pred = regressor.predict(test_data)
print(y_pred)
```

### Ludwig

```python
from ludwig import LudwigModel
import pandas as pd

df = pd.read_csv("home_rentals.csv") # reading data
# defining the data
model_definition = {
    'input_features':[
        {'name':'number_of_rooms', 'type':'numerical'},
        {'name':'number_of_bathrooms', 'type':'numerical'},
        {'name':'sqft', 'type':'numerical'},
        {'name':'location', 'type':'text'},
        {'name':'days_on_market', 'type':'numerical'},
        {'name':'initial_price', 'type':'numerical'},
        {'name':'neighborhood', 'type':'text'},
    ],
    'output_features': [
        {'name': 'rental_price', 'type': 'numerical'}
    ]
}
# creating and training the model
model = LudwigModel(model_definition)
train_stats = model.train(data_df=df)
model.close()
```

*Note: If the data is inconsistent or of erroneous, null value it may throw an error. So you may want to  preprocess/clean  the data first.*

### Mindsdb

```python
import mindsdb

# Instantiate a mindsdb Predictor
mdb = mindsdb.Predictor(name='real_estate_model')

# We tell the Predictor what column or key we want to learn and from what data
mdb.learn(
    from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv", # the path to the file where we can learn from, (note: can be url)
    to_predict='rental_price', # the column we want to learn to predict given all the data in the file
)

mdb = mindsdb.Predictor(name='real_estate_model')

# use the model to make predictions
# Note: you can use the `when_data` argument if you want to use a file with one or more rows instead of a python dictionary
result = mdb.predict(when={'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190})

# The result will be an array containing predictions for each data point (in this case only one), a confidence for said prediction and a few other extra informations
print('The predicted price is ${price} with {conf} confidence'.format(price=result[0]['rental_price'], conf=result[0]['rental_price_confidence']))
```

*Note: In the Mindsdb code sample we did not use the preprocessed data, we directly gave it a link where the data is stored.*

Generally speaking, Mindsdb differentiates itself from other libraries by its **simplicity**. Lastly, it will provide you with the ability to visualize the insights that you get from training in an easy to understand way.
