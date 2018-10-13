
# MindsDB

MindsDB's goal is to make it very simple for developers to use the power of artificial neural networks in their projects. 


## Learn More

* [Why MindsDB?](docs/README.md)
* [Installing MindsDB](docs/Installing.md)
* [Config Settings](docs/Config.md)
* [Learning from Examples](docs/examples/basic)
* [Inside MindsDB](docs/InsideMindsDB.md)



## Getting started

It's very simple to setup [(more)](docs/Installing.md)

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






