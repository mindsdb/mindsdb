import base64
import json
import pickle
from typing import Optional, Dict

import pandas as pd
import requests

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log
from mindsdb.interfaces.storage import db


class RayServeHandler(BaseMLEngine):
    name = 'rayserve'

    ARG_TARGET = "target"
    ARG_COLUMN_SEQUENCE = "column_sequence"
    ARG_TRAIN_URL = "train_url"
    ARG_PREDICT_URL = "predict_url"
    ARG_PREDICTOR_ID = "predictor_id"
    ARG_PREDICTOR_NAME = "predictor_name"

    KWARGS_DF = "df"

    PERSIST_ARGS_KEY_IN_JSON_STORAGE = "rayserve_args"

    def create(self, target, args=None, **kwargs):
        # prepare arguments
        using_args = args.get("using", dict())
        train_url = using_args.get("train_url", None)
        predict_url = using_args.get("predict_url", None)
        if train_url is None:
            raise Exception("Missing required argument: using.train_url")
        if predict_url is None:
            raise Exception("Missing required argument: using.predict_url")

        predictor_id = self.model_storage.predictor_id
        predictor_name = db.Predictor.query.get(predictor_id).name
        # prepare args to serialize
        serialize_args = dict()
        serialize_args[self.ARG_TARGET] = target
        serialize_args[self.ARG_TRAIN_URL] = train_url
        serialize_args[self.ARG_PREDICT_URL] = predict_url
        serialize_args[self.ARG_PREDICTOR_ID] = predictor_id
        serialize_args[self.ARG_PREDICTOR_NAME] = predictor_name
        # check df
        df: pd.DataFrame = kwargs.get(self.KWARGS_DF, None)
        if df is None:
            raise Exception("missing required key in args: " + self.KWARGS_DF)
        else:
            column_sequence = sorted(list(df.columns.values))
            df = df[column_sequence]
            serialize_args[self.ARG_COLUMN_SEQUENCE] = column_sequence

        try:
            resp_status_code = ""
            log.logger.info(F"Training model by RayServe, target: {target}, "
                            F"train_url: {train_url}, predict_url: {predict_url}")
            encoded_df = base64.encodebytes(pickle.dumps(df))
            resp = requests.post(train_url, json={'df': encoded_df, 'target': target,
                                                  "predictor_id": predictor_id, "predictor_name": predictor_name})
            resp_status_code = resp.status_code
            assert resp.status_code == 200
            log.logger.info(F"Training model by RayServe success.")
        except Exception as e:
            raise Exception("Training failed: " + repr(e) + ", status_code: ", str(resp_status_code))
        self.model_storage.json_set(self.PERSIST_ARGS_KEY_IN_JSON_STORAGE, serialize_args)
        log.logger.info("Model args saved.")

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        # read model args from storage
        deserialize_args = self.model_storage.json_get(self.PERSIST_ARGS_KEY_IN_JSON_STORAGE)

        # resolve args
        target = deserialize_args[self.ARG_TARGET]
        feature_column_sequence = list(deserialize_args[self.ARG_COLUMN_SEQUENCE])
        if target in feature_column_sequence:
            feature_column_sequence.remove(target)
        predict_url = deserialize_args[self.ARG_PREDICT_URL]
        predictor_id = deserialize_args[self.ARG_PREDICTOR_ID]
        predictor_name = deserialize_args[self.ARG_PREDICTOR_NAME]

        df_send = df[feature_column_sequence]
        encoded_df = base64.encodebytes(pickle.dumps(df_send))
        resp = requests.post(predict_url, json={'df': encoded_df, 'target': target,
                                                "predictor_id": predictor_id, "predictor_name": predictor_name})
        pred_result = json.loads(resp.content)
        if target not in pred_result:
            raise Exception(F"Target column doesn't exist: {target}")
        if len(pred_result[target]) != len(df_send):
            raise Exception(F"Line number of pre diction doesn't equal with the one of input.")
        rt = df.copy(deep=True)
        pred_df = pd.DataFrame(index=rt.index, data=pred_result)
        if target in set(rt.columns):
            rt.drop(columns=[target], inplace=True)
        rt = rt.join(pred_df, how="left")
        return rt

if __name__=="__main__":
    print()