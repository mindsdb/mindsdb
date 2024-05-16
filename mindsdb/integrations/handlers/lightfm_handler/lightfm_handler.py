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

        # store model parameters
        args["model_params"] = model_parameters.model_dump()

        rec_preprocessor = RecommenderPreprocessor(
            interaction_data=df,
            user_id_column_name=args["user_id"],
            item_id_column_name=args["item_id"],
            threshold=args["threshold"],
        )

        # preprocess data
        preprocessed_data = rec_preprocessor.preprocess()

        args["n_users_items"] = rec_preprocessor.n_users_items

        # get item idx to id and user idx to id maps
        args["item_idx_to_id_map"] = preprocessed_data.idx_item_map
        args["user_idx_to_id_map"] = preprocessed_data.idx_user_map

        random_state = 42

        # run evaluation if specified
        if args.get("evaluation"):

            evaluation_metrics = self.evaluate(
                preprocessed_data.interaction_matrix, random_state, model_parameters
            )
            # convert to float to str so it can be stored in json
            args["evaluation_metrics"] = {
                k: str(v) for k, v in evaluation_metrics.items()
            }

        # train model
        model = LightFM(
            learning_rate=model_parameters.learning_rate,
            loss=model_parameters.loss,
            random_state=random_state,
        )
        model.fit(preprocessed_data.interaction_matrix, epochs=model_parameters.epochs)

        self.model_storage.file_set("model", dill.dumps(model))
        self.model_storage.json_set("args", args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None):

        predict_params = args["predict_params"]

        # if user doesn't specify recommender type, default to user_item
        try:
            recommender_type = df["recommender_type"].tolist()[0]
        except KeyError:
            recommender_type = predict_params.get("recommender_type", "user_item")

        args = self.model_storage.json_get("args")

        # get item_id to idx and user_id to idx maps
        args["item_id_to_idx_map"] = dict(
            zip(args["item_idx_to_id_map"].values(), args["item_idx_to_id_map"].keys())
        )
        args["user_id_to_idx_map"] = dict(
            zip(args["user_idx_to_id_map"].values(), args["user_idx_to_id_map"].keys())
        )

        model = dill.loads(self.model_storage.file_get("model"))

        n_users = args["n_users_items"][0]
        n_items = args["n_users_items"][1]
        item_ids, user_ids = None, None

        if recommender_type == "user_item":
            if df is not None:

                if args["item_id"] in df.columns:
                    n_items = df[args["item_id"]].nunique()
                    item_ids = df[args["item_id"]].unique().tolist()

                if args["user_id"] in df.columns:
                    n_users = df[args["user_id"]].nunique()
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
            if df is not None and args["item_id"] in df.columns:
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

    def describe(self, attribute=None):
        model_args = self.model_storage.json_get("args")

        if attribute == "model":
            return pd.DataFrame({k: [model_args[k]] for k in ["model_params"]})

        elif attribute == "features":
            return pd.DataFrame(
                {
                    "n_users_items": [model_args["n_users_items"]],
                }
            )

        elif attribute == "info":

            model_metrics = model_args["evaluation_metrics"]

            info_dict = {k: [model_metrics[k]] for k in ["auc", "precision", "recall"]}
            info_dict["user_id"] = [model_args["user_id"]]
            info_dict["item_id"] = [model_args["item_id"]]

            return pd.DataFrame(info_dict)

        else:
            tables = ["info", "features", "model"]
            return pd.DataFrame(tables, columns=["tables"])

    def evaluate(self, interaction_matrix, random_state, model_parameters):

        train, test = random_train_test_split(
            interaction_matrix,
            test_percentage=0.2,
            random_state=random_state,
        )
        model = LightFM(
            learning_rate=model_parameters.learning_rate,
            loss=model_parameters.loss,
            random_state=random_state,
        )

        model.fit(train, epochs=model_parameters.epochs)

        evaluation_metrics = dict(
            auc=auc_score(model, test, train).mean(),
            precision=precision_at_k(model, test, train).mean(),
            recall=recall_at_k(model, test, train).mean(),
        )

        return evaluation_metrics
