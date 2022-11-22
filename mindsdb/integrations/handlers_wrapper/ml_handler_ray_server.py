import pandas as pd
from fastapi import FastAPI, Request
from ray import serve
import os
import importlib

from ..handlers_client.ml_ray_client import to_data, to_dataframe

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage


# Use this var to test service inplace update. When the env var is updated, users see new return value.
msg = os.getenv("SERVE_RESPONSE_MESSAGE", "Hello world!")

serve.start(detached=True)

app = FastAPI()

def init():
    # required env vars:
    #   MINDSDB_DB_CON
    #   STORAGE_LOCATION
    # for S3
    #   S3_BUCKET

    db.init()

@serve.deployment(route_prefix="/")
@serve.ingress(app)
class MLHandlerRay:
    def get_ml_handler(self, handler_info):
        module = importlib.import_module(handler_info['module_name'])
        HandlerClass = getattr(module, handler_info['class_name'])

        company_id = handler_info['company_id']
        handlerStorage = HandlerStorage(company_id, handler_info['integration_id'])
        modelStorage = ModelStorage(company_id, handler_info['predictor_id'])

        ml_handler = HandlerClass(
            engine_storage=handlerStorage,
            model_storage=modelStorage,
        )
        return ml_handler

    @app.post("/create")
    def create(self, request: Request):
        data = request.json()
        target = data['target']
        handler_info = data['handler_info']
        df = to_dataframe(data['data'])
        args = data['args']

        ml_handler = self.get_ml_handler(handler_info)

        return ml_handler.create(target, df, args)

    @app.post("/predict")
    def predict(self, request: Request):
        data = request.json()
        handler_info = data['handler_info']
        df = to_dataframe(data['data'])
        args = data['args']

        ml_handler = self.get_ml_handler(handler_info)

        predicted = ml_handler.predict(df, args)
        return to_data(predicted)

    @app.get("/healthcheck")
    def healthcheck(self):
        return

MLHandlerRay.deploy()
