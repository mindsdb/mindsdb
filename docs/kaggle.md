[<Back to Table of Contents](../README.md)

# Using MindsDB with Kaggle House Prices dataset
Kaggle allows users to find and publish data sets, explore and build models in a web-based data-science environment.
There are thousands of datasets available for training the models.
You can signin or register on https://www.kaggle.com with Google, Facebook, Yahoo or your email.

## House prices dataset
For this example we will use [house prices dataset.](https://www.kaggle.com/lespin/house-prices-dataset)

For training use [train.csv](https://www.kaggle.com/lespin/house-prices-dataset#train.csv) dataset.

## Install mindsdb
First, install mindsdb [installation guide](https://github.com/ZoranPandovski/mindsdb/blob/master/docs/Installing.md) by following the installation guide.

## Train model
Create a new python file called train.py, and inside create new model:
```Python
from mindsdb import *

# We tell mindsDB what we want to learn and from what data
MindsDB().learn(
    from_file='train.csv', # 
    predict='SalePrice', 
    model_name='kaggle_house_sale'
)
```
* **from_file** shows mindsdb the path to the train.csv dataset
* **predict** The column we want to learn to predict given all the data in the file. Note this column doesn't exist in train.csv
* **model_name** The name of the model we are training

## Use the model and predict data
Create new file e.g predict.py and inside use the newly created kaggle_house_sale model.
```Python
from mindsdb import *

# First we initiate MindsDB
mdb = MindsDB()

# Here we use the model to make predictions (NOTE: You need to run train.py first)
result = mdb.predict(predict='SalePrice', when={"MSSubClass": 20, "MSZoning": "Rh","LotFrontage":80,"LotArea":11622}, model_name='house_sale')

print(result.predicted_values)
# you can now print the results
print('The predicted price is ${price} with {conf} confidence'.format(price=result.predicted_values[0]['SalePrice'], conf=result.predicted_values[0]['prediction_confidence']))

```
In when clause you can specify different input parameters with different values. 
Open kaggle house prices dataset page and check [data_description file](https://www.kaggle.com/lespin/house-prices-dataset#data_description.txt).
Inside data_description.txt file you can find full data description and how to use those values for predicting new price.
Example: 

* "MSZoning":"A" means Agriculture, 
* "MSZoning":"C" means Commercial 
* "OverallQual": 10 means Very Excellent etc

## Contributions
Feel free to investigate and find new datasets in kaggle, use them to train new models with mindsdb and share the examples.


