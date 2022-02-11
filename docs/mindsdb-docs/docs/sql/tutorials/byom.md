# Bring Your Own Model

Mindsdb allows you to integrate your own machine learning models into it.

In order to do this your model will require some sort of API wrapper, for now we have 2x API specifications we support, mlflow and ray serve.

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

## MLFlow

### Train 

You cannot train models served by MLFlow through mindsdb.

### Predict

Already-trained MLFlow models can be used from within Mindsdb.