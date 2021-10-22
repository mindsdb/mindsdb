---
id: comparison-mindsdb
title: Comparison with other Libraries
---

Let's compared Mindsdb with some popular deep learning and machine learning libraries to show what makes it different.

## Models

With libraries such as Tensorflow, Sklearn, Pytorch you must have the expertise to build models from scratch. Your models are also black boxes, you can't be sure how or why they work and you have to pre-process your data in a format that's suitable for the model and look for any errors in the data yourself.

With Mindsdb anyone can build state of the art models without any machine learning knowledge. Mindsdb also provides data extraction, analyses your input data and analyses the resulting model to try and understand what makes it work and what types of situations it works best in.

## Data Preprocessing

Building models require time for cleaning the data, normalizing the data, converting it into the format your library uses, determining the type of data in each column and a proper encoding for it

Mindsdb can read data from csv, json, excel, file urls, s3 objects , dataframes and relational database tables or queries (currently there's native support for maraiadb, mysql and postgres), you just need to tell it which column(s) it should predict. It will automatically process the data for you and give insights about the data.


## Code Samples

We are going to use **[home_rentals.csv](https://github.com/mindsdb/mindsdb-examples/tree/master/benchmarks/home_rentals)** dataset for comparison purpose. Inside the `dataset` dir, you can find the dataset split into `train` and `test` data.

<iframe width="560" height="315" src="https://www.youtube.com/embed/tLaLfaIJf-Y" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

Our goal is to predict the rental_price of the house given the information we have in `home_rental.csv`.

We will look at doing this with Sklearn, Tensorflow, Ludwig and Mindsdb.

* Sklearn is a generic easy-to-use machine learning library.
* Tensorflow is the state of the art deep learning model building library from google.
* Ludwig is a library from Uber that aims to help people build machine learning models without knowledge of machine learning (similar to mindsdb)

### Building the model

Now we will build the actual models to train on the training dataset and run some predictions on the testing dataset. For the purpose of this example, we'll build a simple linear regression with both Tensorflow and Sklearn, in order to keep the code to a minimum.

#### Tensorflow

```python
import tensorflow as tf
import pandas as pd
import numpy as np
from tensorflow import keras
from tensorflow.keras import layers
from sklearn.preprocessing import MinMaxScaler

# process data
df = pd.read_csv("home_rentals.csv")
labels = df.pop('rental_price').values.reshape(-1, 1)
features = df._get_numeric_data().values

xscaler = MinMaxScaler()
features = xscaler.fit_transform(features)

yscaler = MinMaxScaler()
labels = yscaler.fit_transform(labels)

input_dim = 4  # only numerical features for this example
output_dim = 1 # predict rental_price

# neural network definition
inputs = keras.Input(shape=(4))
x = layers.Dense(100)(inputs)
outputs = layers.Dense(1)(x)
model = keras.Model(inputs=inputs, outputs=outputs)
optimizer = keras.optimizers.SGD(learning_rate=1e-4)

# transform data to TensorFlow format
dataset = tf.data.Dataset.from_tensor_slices((features.astype(np.float32), 
                                              labels.astype(np.float32)))
dataset = dataset.shuffle(buffer_size=64).batch(32)

def compute_loss(labels, predictions):
  return tf.reduce_mean(tf.square(labels - predictions))  # mean squared error

def train_on_batch(x, y):
  with tf.GradientTape() as tape:
    predictions = model(x)
    loss = compute_loss(y, predictions)
    gradients = tape.gradient(loss, model.trainable_weights)
    optimizer.apply_gradients(zip(gradients, model.trainable_weights))
  return loss  

epochs = 50
for epoch in range(epochs):
  for step, (x, y) in enumerate(dataset):
    loss = train_on_batch(x, y)
  if epoch % 10 == 0:
    print(f'Epoch {epoch}: last batch loss = {float(loss)}')

# predict for test sample
feat = [[2,     # rooms
         1,     # bathrooms
         1190,  # square feet
         2000]] # initial price

print("The predicted price is %f " % yscaler.inverse_transform(
    model.predict(xscaler.transform(feat))))
```

#### Sklearn

```python
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import pandas as pd

#load data
data = pd.read_csv("home_rentals.csv")
#target value
labels = data['rental_price']
train1 = data.drop(['rental_price'],axis=1)

#train test split
x_train , x_test , y_train , y_test = train_test_split(train1 , labels)

# label encode values
le = LabelEncoder()
le.fit(x_train['location'].astype(str))
x_train['location'] = le.transform(x_train['location'].astype(str))
x_test['location'] = le.transform(x_test['location'].astype(str))

le.fit(x_train['neighborhood'].astype(str))
x_train['neighborhood'] = le.transform(x_train['neighborhood'].astype(str))
x_test['neighborhood'] = le.transform(x_test['neighborhood'].astype(str))

# Create linear regression object
regr = linear_model.LinearRegression()

# Train the model using the training sets
regr.fit(x_train, y_train)

# Make predictions using the testing set
prediction = regr.predict(x_test)

# The coefficients
print('Prediction ', prediction)
print('Coefficients: \n', regr.coef_)

# The mean squared error
print('Mean squared error: %.2f'
      % mean_squared_error(y_test, prediction))
# The coefficient of determination: 1 is perfect prediction
print('Coefficient of determination: %.2f'
      % r2_score(y_test, prediction))
```

#### Ludwig

```python
from ludwig.api import LudwigModel

import pandas as pd

# read data
train_dataf = pd.read_csv("train.csv") 
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
train_stats = model.train(data_df=train_dataf)

# read test data
test_dataf = pd.read_csv("test.csv")

#predict data
predictions = model.predict(data_df=test_dataf)

print(predictions)
model.close()
```

*Note: If the data is inconsistent or of erroneous, null value it may throw an error. So you may want to  preprocess/clean  the data first.*

### Mindsdb Native

```python
from mindsdb import Predictor

# tell mindsDB what we want to learn and from what data
Predictor(name='home_rentals_price').learn(
    to_predict='rental_price', # the column we want to learn to predict given all the data in the file
    from_data='train.csv' # the path to the file where we can learn from, (note: can be url)
)
# use the model to make predictions
result = Predictor(name='home_rentals_price').predict(when_data={'number_of_rooms': 2, 'initial_price': 2000, 'number_of_bathrooms':1, 'sqft': 1190})

# now print the results
print('The predicted price is between ${price} with {conf} confidence'.format(price=result[0].explanation['rental_price']['confidence_interval'], conf=result[0].explanation['rental_price']['confidence']))
```

Generally speaking, Mindsdb differentiates itself from other libraries by its **simplicity**. Lastly, [Mindsdb Studio](/model/train/) provides you with an easy way to visiualize more insights about the model. This can also be done by calling `mdb.get_model_data('model_name')`, but it's easier to use Mindsdb Studio to visualize the data, rather than looking at the raw json.


# Comparing Mindsdb accuracy with state-of-the-art models

We have a few example datasets where we try to compare the accuracy obtained by Mindsdb with that of the best models we could find.

It should be noted, Mindsdb accuracy doesn't always beat or match stat-of-the-art models, but the main goal of Mindsdb is to learn quickly, be very easy to use and be adaptable on any dataset.

If you have the time and know-how to build a model that performs better than Mindsdb, but you still want the insights into the model and the data, as well as the pre-processing that Mindsdb provides, you can always plug in a custom machine learning model into Mindsdb.

We are currently creating a new Benchmarks repository where you can find detailed list with up to date examples. Until we release that you can check the old list of accuracy comparisons on our [examples repo](https://github.com/mindsdb/mindsdb-examples/tree/master/benchmarks) and [public benchmarks](https://github.com/mindsdb/benchmarks).

Each directory contains different examples, datasets and `README.md`. To see the accuracies and the models, simply run `mindsdb_acc.py` to run mindsdb on the dataset.

At some point we might keep a more easy to read list of these comparisons, but for now the results change to often and there are too many models to make this practical to maintain.

We invite anyone with an interesting dataset and a well performing models to send it to us, or contribute to this repository, so that we can see how mindsdb stands up to it (or try it themselves and tell us the results they got).