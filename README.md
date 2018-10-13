
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
from mindsdb import MindsDB

mdb = MindsDB()
mdb.learn(
    from_file='monthly_sales.csv',
    predict='sales',
    model_name='sales_model'
)
```

To **use a Model**:

```python
from mindsdb import MindsDB

mdb = MindsDB()
predicted_sales = mdb.predict(predict='sales', when={'month': 'Sept'}, model_name='sales_model')

```






