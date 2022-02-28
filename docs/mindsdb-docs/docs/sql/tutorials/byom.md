# Bring Your Own Model

Mindsdb allows you to integrate your own machine learning models into it.

In order to do this your model will require some sort of API wrapper, for now we have 2x API specifications we support, mlflow and ray serve.

The former supports importing already trained models and predicting with them from mindsdb. The later supports both training and predicting with external models.

In order to use custom models there are three mandatory arguments one must past inside the `USING` statement:
- `url.predict`, this is the url to call for getting predictions from your model
- `format`, this can be == `mlflow` or `ray_serve`
- `dtype_dict`, this is a json specifying all columns expected by their models and their types

There's an additional optional argument if you want to train the model via mindsdb:
- `url.train`, which is the endpoint we'll call when training your model

## Ray Server

Ray serve is a simple high-thoroughput service that can wrap over your own ml models.

```
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


MyModel.deploy()

while True:
    time.sleep(1)
```

The important bits here are having a train and predict endpoint.

The `train` endpoint accept two parameters in the json sent via POST:
- `df` -- a serialized dictionary that can be converted into a pandas dataframe
- `target` -- the name of the target column

The `predict` endpoint needs only one parameter:
- `df` -- a serialized dictionary that can be converted into a pandas dataframe


The training endpoints must return a json that contains the keys `status` set to `ok`. The predict endpoint must return a dictionayr containing the `prediction` key, storing the predictions. Additional keys can be returned for confidence and confidence intervals.

Once you start this ray serve wrapped model you can train it via the query:

```
CREATE PREDICTOR byom
FROM mydb (
    SELECT number_of_rooms, initial_price, rental_price 
    FROM test_data.home_rentals
) PREDICT number_of_rooms
USING
url.train = 'http://127.0.0.1:8000/my_model/train',
url.predict = 'http://127.0.0.1:8000/my_model/predict',
dtype_dict={"number_of_rooms": "categorical", "initial_price": "integer", "rental_price": "integer"},
format='ray_server';
```

And you can query predictions as usual:

```
SELECT * FROM byom WHERE initial_price=3000 AND rental_price=3000;
```

or by JOINING

```
SELECT tb.number_of_rooms, t.rental_price FROM mydb.test_data.home_rentals AS t JOIN mindsdb.byom AS tb WHERE t.rental_price > 5300;
```

*Please note that, if your model is behind a reverse proxy (e.g. nginx) you might have to increase the maximum limit for POST requests in order to receive the training data. Mindsdb itself can send as much that as you'd like and has been stress-tested with over a billion rows.*

## MLFlow

MLFlow is a tool that you can use to train and serve models.

As training is done through code rather than the API, that bit you will have to do from outside of mindsdb by pulling your data manually.

The first step would be to install mlflow and get a model going, you can use the one in this very simple tutorial: https://github.com/mlflow/mlflow#saving-and-serving-models

Next, we're going to add this to mindsdb:

```
CREATE PREDICTOR byom2 PREDICT `1` USING url.predict='http://localhost:5000/invocations', format='mlflow', data_dtype={"0": "integer", "1": "integer"}
```

Then we can run predictions as usual, by using the `WHERE` statement or joining on a data table with an appropriate schema:

```
SELECT y FROM byom2 WHERE `0`=2;
```