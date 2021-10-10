
!!! info "Work In Progress documentation :warning: :construction: :construction_worker: "
    Note that the documentation for MindsDB SDK's is under development. If you found missing feature or something isn't working please reach out to us on the [Python SDK](https://github.com/mindsdb/mindsdb_python_sdk/issues/new/choose) or [JS SDK](https://github.com/mindsdb/mindsdb_js_sdk/issues/new/choose) repositories.

!!! info "MindsDB Server"
    Note that to use MindsDB SDK, you will need to have MindsDB Server started so you can connect to it.


The MindsDB SDK's are providing all of the MindsDB's native functionalities through MindsDB HTTP Interface. Currently, MindsDB provides SDK's for Python and JavaScript.

## Installing Python SDK

The Python SDK can be installed from PyPI:

```
pip install mindsdb_sdk
```

Or you can install it from source:

```
git clone git@github.com:mindsdb/mindsdb_python_sdk.git
cd mindsdb_python_sdk
python setup.py develop
pip install -r requirements.txt
```

## Connect to your data

DataSources make it very simple to connect MindsDB to your data. Datasource can be:

* File(csv, tsv, json, xslx, xls)
* Pandas dataframe 
* MindsDB datasource that is an enriched version of a pandas dataframe. MindsDB datasource could be MariaDB, Snowflake, S3, Sqlite3, Redshift, PostgreSQL, MsSQL, MongoDB, GCS, Clickhouse, AWS Athena. For more info please check the full list of datasource implementation [here](https://github.com/mindsdb/datasources/tree/stable/mindsdb_datasources/datasources).

### Create new datasource from local file

Before you train new model you need to create a datasource, so MindsDB can ingest and prepare the data. The following example use [Medical Cost dataset](https://www.kaggle.com/mirichoi0218/insurance). Let's load the local file as an pandas dataframe and create new datasource:

```python
from mindsdb_sdk import SDK # import SDK
import pandas as pd # import pandas
mindsdb_sdk = SDK('http://localhost:47334') # Connect to MindsDB Server URL
datasources = mindsdb_sdk.datasources 
df = pd.read_csv('datasets/insurance.csv') # read the dataset
datasources['health_insurance'] = {'df': df} # create new datasource
```

To check that the datasource was successfully created, you can call the `list_info()` method from datasources:

```python
datasources.list_info()
```

This will return the info for each datasource as the following example:

```json
{
    'name': 'health_insurance',
    'source_type': 'file',
    'source': '/mindsdb/var/datasources/health_insurance/',
    'missed_files': None,
    'created_at': '2021-02-19T14:20:57.860292',
    'updated_at': '2021-02-19T14:20:57.908497',
    'row_count': 27,
    'columns': [{
        'name': 'age',
        'type': None,
        'file_type': None,
        'dict': None
    } ... other columns
}
```

### Train new model

The `learn` method is used to make the predictor learn from the data. The required arguments to `learn` from data are `model_name`, `target_variable`, `datasource`.
The following example train new model called `insurance_model` that predicts the `charges` from `health_insurance` datasource.

```python
model = mindsdb_sdk.predictors
model.learn('insurance_model',  'charges', 'health_insurance',)
```


### Query the model

To get the predictons from the model use the `predict` method. This method is used to make predictions and it can take `when_data` argument. This can be file, dataframe or url or if you want to make the single predicton as in the following example use a dictionary.


```python
model.predict(when_data={'age': 30, 'bmi': 22})
```

## Usage example

The following example trains new model from [home_rentals dataset](https://github.com/mindsdb/benchmarks/blob/main/datasets/home_rentals/data.csv) and predicts the rental price.

```python
from mindsdb_sdk import SDK

# connect
mdb = SDK('http://localhost:47334')

# upload datasource
mdb.datasources['home_rentals_data'] = {'file' : 'home_rentals.csv'}

# create a new predictor and learn to predict
predictor = mdb.predictors.learn(
    name='home_rentals',
    datasource='home_rentals_data',
    to_predict='rental_price'
)

# predict
result = predictor.predict({'initial_price': '2000','number_of_bathrooms': '1', 'sqft': '700'})
```


## Installing JavaScript SDK

The JavaScript SDK can be installed with npm or yarn:

```
npm install mindsdb-js-sdk
```
or

```
yarn add mindsdb-js-sdk
```

Also, you can install it from source:

```
git clone git@github.com:mindsdb/mindsdb_js_sdk.git
cd mindsdb_js_sdk
npm install
```

## Usage example

The following example covers the basic flow: connect to MindsDB Server, train new model, make predictions.

```javascript
import MindsDB from 'mindsdb-js-sdk';

//connection
MindsDB.connect(url);
const connected = await MindsDB.ping();
if (!connected) return;

// lists of predictors and datasources
const predictorsList = MindsDB.dataSources();
const predictors = MindsDB.predictors();

// get datasource
const rentalsDatasource = await MindsDB.DataSource({name: 'home_rentals'}).load();

// get predictor
const rentalsPredictor = await MindsDB.Predictor({name: 'home_rentals'}).load();

// query
const result = rentalsPredictor.queryPredict({'initial)|_price': 2000, 'sqft': 500});
console.log(result);

MindsDB.disconnect();
```
