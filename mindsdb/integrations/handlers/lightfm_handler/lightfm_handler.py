from typing import Dict, Optional

import dill
import pandas as pd
from dataprep_ml.recommenders import RecommenderPreprocessor
from lightfm import LightFM
from lightfm.cross_validation import random_train_test_split
from lightfm.evaluation import auc_score, precision_at_k, recall_at_k

from mindsdb.integrations.handlers.lightfm_handler.helpers import (
    get_item_item_recommendations,
    get_user_item_recommendations,
)
from mindsdb.integrations.handlers.lightfm_handler.settings import ModelParameters
from mindsdb.integrations.libs.base import BaseMLEngine


class LightFMHandler(BaseMLEngine):
    """
    Integration with the lightfm Recommender library.
    """

    name = "lightfm"

    # todo add ability to partially update model based on new data for existing users, items
    # todo add support for hybrid recommender
    def create(self, target: str, df: pd.DataFrame = None, args: Optional[Dict] = None):

        args = args["using"]

        # get model parameters if defined by user - else use default values

        user_defined_model_params = list(
            filter(lambda x: x in args, ["learning_rate", "loss", "epochs"])
        )
        args["model_params"] = {
            model_param: args[model_param] for model_param in user_defined_model_params
        }
        model_parameters = ModelParameters(**args["model_params"])

        #
        rec_preprocessor = RecommenderPreprocessor(
            interaction_data=df,
            user_id_column_name=args["user_id"],
            item_id_column_name=args["item_id"],
            threshold=args["threshold"],
        )

        # todo update imports once merged into dataprep_ml
        # preprocess data
        preprocessed_data = rec_preprocessor.preprocess()

        args["n_users_items"] = rec_preprocessor.n_users_items

        # get item idx to id and user idx to id maps
        args["item_idx_to_id_map"] = preprocessed_data.idx_item_map
        args["user_idx_to_id_map"] = preprocessed_data.idx_user_map

        # get item_id to idx and user_id to idx maps
        args["item_id_to_idx_map"] = dict(
            zip(args["item_idx_to_id_map"].values(), args["item_idx_to_id_map"].keys())
        )
        args["user_id_to_idx_map"] = dict(
            zip(args["user_idx_to_id_map"].values(), args["user_idx_to_id_map"].keys())
        )

        # run evaluation if specified
        if args.get("evaluate", False):
            train, test = random_train_test_split(
                preprocessed_data.interaction_matrix,
                test_percentage=0.2,
                random_state=42,
            )
            model = LightFM(
                learning_rate=model_parameters.learning_rate,
                loss=model_parameters.loss,
                random_state=42,
            )

            model.fit(train, epochs=model_parameters.epochs)

            evaluation_metrics = dict(
                auc=auc_score(model, test, train).mean(),
                precision=precision_at_k(model, test, train).mean(),
                recall=recall_at_k(model, test, train).mean(),
            )
            self.model_storage.json_set("evaluation_metrics", evaluation_metrics)

        # train model
        model = LightFM(
            learning_rate=model_parameters.learning_rate,
            loss=model_parameters.loss,
            random_state=42,
        )
        model.fit(preprocessed_data.interaction_matrix, epochs=model_parameters.epochs)

        self.model_storage.file_set("model", dill.dumps(model))
        self.model_storage.json_set("args", args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None):

        predict_params = args["predict_params"]

        # if user doesn't specify recommender type, default to user_item
        recommender_type = predict_params.get("recommender_type", "user_item")

        args = self.model_storage.json_get("args")
        model = dill.loads(self.model_storage.file_get("model"))

        n_users = args["n_users_items"][0]
        n_items = args["n_users_items"][1]
        item_ids, user_ids = None, None

        if recommender_type == "user_item":
            if df is not None:
                n_items = df[args["item_id"]].nunique()
                n_users = df[args["user_id"]].nunique()
                item_ids = df[args["item_id"]].unique().tolist()
                user_ids = df[args["user_id"]].unique().tolist()

            return get_user_item_recommendations(
                n_users=n_users,
                n_items=n_items,
                args=args,
                model=model,
                item_ids=item_ids,
                user_ids=user_ids,
            )

        elif recommender_type == "item_item":
            if df is not None:
                item_ids = df[args["item_id"]].unique().tolist()

            return get_item_item_recommendations(
                model=model,
                args=args,
                item_ids=item_ids,
            )

        elif recommender_type == "user_user":
            raise NotImplementedError(
                "user_user recommendation type is not implemented yet"
            )

        else:
            raise ValueError(
                "recommender_type must be either 'user_item', 'item_item' or 'user_user'"
            )
