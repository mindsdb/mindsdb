[<Back to Table of Contents](README.md)
# Using MindsDB

MindsDB objective is that you can build powerful predictive models with just a few lines of code.

To illustrate this:
Asume that you have a CSV File of sales, you would like to learn how to best predict sales given the data 
To train a model:

```python
from mindsdb import MindsDB

mdb = MindsDB()
mdb.learn(
    from_file='monthly_sales.csv',
    predict='sales',
    model_name='sales_model'
)
```

To use a Model:

```python
from mindsdb import MindsDB

mdb = MindsDB()
predicted_sales = mdb.predict(predict='sales', when={'month': 'Sept'}, model_name='sales_model')

```

But there is much more to MindsDB. Please read play with the examples.