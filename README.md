
# MindsDB

MindsDB's goal is to make it very simple for developers to use the power of artificial neural networks in their projects. 

## Getting started

It's very simple to setup

```bash
 pip3 install mindsdb --user
```



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

## Learn More



You can learn more in the [MindsDB Quick docs](docs/README.md)



