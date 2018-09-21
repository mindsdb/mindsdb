
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


You can learn more in the [MindsDB Quick docs](docs/README.md)





### Why use MindsDB?

Developers today are more aware of the capabilities of Machine Learning, however from ideas of using ML to actual implementations,  there are many hurdles and therefore most ideas of using Machine Learning never even start.

Thanks to MindsDB people building products can **focus more on**:

* Understanding what problems/predictions are interesting for the business.
* What data should be of interest for a given prediction.

**Less on:**  spending countless hours building models, making data fit into such models, trainining, testing, validating, tunning hyperparameters, ....


