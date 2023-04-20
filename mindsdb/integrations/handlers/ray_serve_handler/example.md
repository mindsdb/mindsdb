# Preliminaries

To use Ray Serve models through MindsDB, you need to create a file that uses Ray Serve to deploy both training and inference endpoints for MindsDB to hook into.

There are several (slightly different) ways to achieve this, but we recommend the approach used in our [docs page](https://docs.mindsdb.com/custom-model/ray-serve):

```python
import ray
from fastapi import Request, FastAPI
from ray import serve

# [other model-specific imports here]

app = FastAPI()
ray.init()
serve.start(detached=True)


async def parse_req(request: Request):
    """Parse a json payload from a post request into a dataframe and target column."""""
    data = await request.json()
    target = data.get('target', None)
    di = json.loads(data['df'])
    df = pd.DataFrame(di)
    return df, target


@serve.deployment(route_prefix="/my_model")
@serve.ingress(app)
class MyModel:
    @app.post("/train")
    async def train(self, request: Request):
        df, target = await parse_req(request)
        # [...model fitting logic here]
        return {'status': 'ok'}

    @app.post("/predict")
    async def predict(self, request: Request):
        df, _ = await parse_req(request)
        # [...model inference logic here]
        pred_dict = {'prediction': [x for x in predictions]}
        return pred_dict


if __name__ == '__main__':
    MyModel.deploy()
    while True:
        time.sleep(1)
```

Once your script looks similar to the one above, save it (e.g. as `model.py`) and setup the server by simply running the file:

```bash
python model.py
```

# MindsDB commands

To create the engine:

```sql
CREATE ML_ENGINE rayserve FROM ray_serve;
```

While the Ray Serve is active, you can use the following command to create the model inside MindsDB by triggering the train endpoint:

```sql
CREATE MODEL mindsdb.rayserve_model
FROM integration_name (SELECT * FROM table_name)
PREDICT target
USING 
engine='ray_serve',
train_url='http://ray_serve_url:port/my_model/train',
predict_url='http://ray_serve_url:port/my_model/predict';
```

In a local deployment, `ray_serve_url = localhost` and `port = 8000`. The `my_model` part of the URL is the `route_prefix` in the script above.

Once the model is created, you should wait until the training process finishes. 

Then, you can query it as any other MindsDB model:
```sql
SELECT input_col, target_col
FROM rayserve_model
WHERE input_col=some_value; -- could also use a JOIN here, as usual

DESCRIBE rayserve_model;
```

# End to end example

Here, we take a look at a simple linear regression done on a single feature to predict a numerical target.

`model.py` looks like this:

```python
import ray
from fastapi import Request, FastAPI
from ray import serve
import time
import pandas as pd
import json
from sklearn.linear_model import LogisticRegression


app = FastAPI()
ray.init()
serve.start(detached=True)


async def parse_req(request: Request):
    data = await request.json()
    target = data.get('target', None)
    di = json.loads(data['df'])
    df = pd.DataFrame(di)
    return df, target


@serve.deployment(route_prefix="/my_model")
@serve.ingress(app)
class MyModel:
    @app.post("/train")
    async def train(self, request: Request):
        df, target = await parse_req(request)
        feature_cols = list(set(list(df.columns)) - set([target]))
        self.feature_cols = feature_cols
        X = df.loc[:, self.feature_cols]
        Y = list(df[target])
        self.model = LogisticRegression()
        self.model.fit(X, Y)
        return {'status': 'ok'}

    @app.post("/predict")
    async def predict(self, request: Request):
        df, _ = await parse_req(request)
        X = df.loc[:, self.feature_cols]
        predictions = self.model.predict(X)
        pred_dict = {'prediction': [float(x) for x in predictions]}
        return pred_dict


if __name__ == '__main__':
    MyModel.deploy()

    while True:
        time.sleep(1)
```

And the MindsDB commands to train and query the model are:

```sql
CREATE ML_ENGINE rayserve FROM ray_serve;  -- assumes user hasn't registered the engine prior to this example

SELECT sqft, rental_price FROM example_db.demo_data.home_rentals LIMIT 10;  -- toy dataset. we'll use `sqft` as the input feature and `rental_price` as the target

CREATE MODEL mindsdb.rayserve_model
FROM example_db (SELECT sqft, rental_price FROM demo_data.home_rentals LIMIT 10)
PREDICT rental_price
USING 
engine='ray_serve',
train_url='http://127.0.0.1:8000/my_model/train',
predict_url='http://127.0.0.1:8000/my_model/predict';

SELECT sqft, rental_price
FROM rayserve_model
WHERE sqft=917;

DESCRIBE rayserve_model;
```

By the end of the entire process, you should get back a response with the predicted `rental_price`:

| sqft | rental_price |
|------|--------------|
|  917 | 3901         |