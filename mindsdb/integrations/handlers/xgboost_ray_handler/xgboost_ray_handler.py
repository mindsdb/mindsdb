import tempfile
from mindsdb.integrations.libs.base import BaseMLEngine
from type_infer.infer import infer_types
from type_infer.dtype import dtype


### original imports
import os
import ast
import glob
import pandas as pd
import xgboost as xgb
from datetime import datetime

import xgboost_ray
from ray import tune
from ray.air.config import RunConfig, ScalingConfig
from xgboost_ray import RayDMatrix, RayParams, RayFileType, train, predict


class XGBoostRayHandler(BaseMLEngine):
    """Integration with the XGBoost-Ray library for
    scalable XGBoost model training and inference.
    """

    name = "xgboost_ray_handler"

    def create(self, target, df, args={}):
        using_args = args["using"]
        model_args = {}
        model_args["model_folder"] = tempfile.mkdtemp()

        # type checking
        dtypes = infer_types(df, 0).to_dict()["dtypes"]
        is_categorical = dtypes[target] in (dtype.categorical, dtype.tags, dtype.binary)
        is_numerical = dtypes[target] in (dtype.integer, dtype.float, dtype.quantity)
        if not is_categorical and not is_numerical:
            raise Exception("Target column must be either categorical or numerical.")

        if is_categorical:
            loss = "multi:softmax"
            n_class = len(df[target].unique())
        else:
            loss = "reg:squarederror"

        # train_x, train_y should both be numpy arrays
        # x shape: (n_rows, n_features); y shape: (n_rows, )

        if using_args.get('mode', 'df'):
            columns = df.columns.tolist()
            df = df.dropna()

            train_y = df.pop(target).values
            train_x = df.values
            train_set = RayDMatrix(train_x, train_y)

        elif using_args.get('mode', 'parquet'):
            dataset = args["dataset"]  # TODO better mechanism for this
            columns = args['columns']  # TODO better mechanism for this
            path = list(sorted(glob.glob(f"./{dataset}_*.parquet")))
            train_set = RayDMatrix(
                path,
                label=target,
                columns=columns,
                enable_categorical=is_categorical,
                filetype=RayFileType.PARQUET)

        # GPU check   # TODO: automatic runtime check here
        use_gpu = False
        if use_gpu:
            config = {
                "tree_method": "gpu_hist",
                'gpu_id': 0
            }
        else:
            config = {
                "tree_method": "approx",
                # "objective": "binary:logistic",
                # "eval_metric": ["logloss", "error"],
                # "eta": tune.loguniform(1e-4, 1e-1),
                # "subsample": tune.uniform(0.5, 1.0),
                # "max_depth": tune.randint(1, 9)
            }

        # Other param settings
        num_actors = args.get('num_actors', 4)
        num_cpus_per_actor = args.get('num_cpus_per_actor', 4)

        ray_params = RayParams(
            num_actors=num_actors,
            cpus_per_actor=num_cpus_per_actor,
            elastic_training=True,
            max_failed_actors=num_actors // 2,
            max_actor_restarts=3,
        )

        # Train model
        evals_result = {}
        start_ts = datetime.now()

        bst = train(
            config,
            train_set,
            evals_result=evals_result,
            evals=[(train_set, "train")],
            verbose_eval=False,
            ray_params=ray_params)

        model_filename = f"xgbr_model_{start_ts}.xgb"
        bst.save_model(model_filename)

        model_args["runtime"] = (datetime.now() - start_ts).total_seconds()

        self.model_storage.json_set("model_args", model_args)

    def predict(self, df, args={}):
        # Load model arguments
        model_args = self.model_storage.json_get("model_args")

        groups_to_keep = prediction_df["unique_id"].unique()

        nf = NeuralForecast.load(model_args["model_folder"])
        forecast_df = nf.predict(prediction_df)
        forecast_df = forecast_df[forecast_df.index.isin(groups_to_keep)]
        return get_results_from_nixtla_df(forecast_df, model_args)
