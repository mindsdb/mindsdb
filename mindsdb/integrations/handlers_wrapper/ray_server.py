import pandas as pd
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import json
from ray import serve
import os
import importlib


from mindsdb.integrations.handlers_client.ray_client import Serializer

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage

# required env vars:
#   MINDSDB_DB_CON
#   S3_BUCKET

# to run locally:
# env MINDSDB_DB_CON=... STORAGE_LOCATION=local serve run mindsdb.integrations.handlers_wrapper.ray_server:entrypoint

# to deploy
# create service.yaml:
#     entrypoint: python ray_server.py
#     runtime_env:
#       working_dir: https://...
#     healthcheck_url: "/check_connection"

# anyscale service deploy service.yaml


app = FastAPI()


class CustomJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return Serializer.json_encode(content).encode("utf-8")


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

    async def get_request(self, request):
        body = await request.body()
        return Serializer.json_decode(body)

    @app.post("/create")
    async def create(self, request: Request):
        db.init()

        data = await self.get_request(request)

        target = data['target']
        handler_info = data['handler_info']
        df = Serializer.dict_to_df(data['data'])
        args = data['args']

        ml_handler = self.get_ml_handler(handler_info)

        db.session.commit()
        return CustomJSONResponse(ml_handler.create(target, df, args))

    @app.post("/predict")
    async def predict(self, request: Request):
        db.init()

        data = await self.get_request(request)

        handler_info = data['handler_info']
        df = Serializer.dict_to_df(data['data'])
        args = data['args']

        ml_handler = self.get_ml_handler(handler_info)

        predicted = ml_handler.predict(df, args)
        return CustomJSONResponse(Serializer.df_to_dict(predicted))

    @app.get("/check_connection")
    def healthcheck(self):
        return
