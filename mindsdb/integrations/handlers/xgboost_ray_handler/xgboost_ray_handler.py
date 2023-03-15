import glob
import dill
import tempfile
from datetime import datetime

import ray
import pandas as pd
# from ray import tune
from type_infer.dtype import dtype
from type_infer.infer import infer_types
from xgboost_ray import RayDMatrix, RayParams, RayFileType, train, predict

from mindsdb.integrations.libs.base import BaseMLEngine

# TODO: rm
# import xgboost_ray
# from ray.air.config import RunConfig, ScalingConfig


class XGBoostRayHandler(BaseMLEngine):
    """Integration with the XGBoost-Ray library for
    scalable XGBoost model training and inference.
    """

    name = "xgboost_ray_handler"

    def create(self, target, df, args={}):
        using_args = args["using"]
        model_args = {
            'target': target,
            "model_folder": tempfile.mkdtemp(),
            'dtypes': infer_types(df, 0).to_dict()["dtypes"]
        }

        # type checking
        is_categorical = model_args['dtypes'][target] in (dtype.categorical, dtype.tags, dtype.binary)
        is_numerical = model_args['dtypes'][target] in (dtype.integer, dtype.float, dtype.quantity)
        if not is_categorical and not is_numerical:
            raise Exception("Target column must be either categorical or numerical.")

        if is_categorical:
            loss = "multi:softmax"
            n_class = len(df[target].unique())
        else:
            loss = "reg:squarederror"
            n_class = None

        if using_args.get('mode', 'df') == 'df':
            df = df.dropna()
            train_y = df.pop(target)    # shape: (n_rows, )
            train_x = df                # shape: (n_rows, n_features)
            train_y.columns = [target]
            model_args['input_cols'] = list(train_x.columns)
            train_set = RayDMatrix(train_x, train_y)

        elif using_args.get('mode', 'df') == 'parquet':
            dataset = args["dataset"]  # TODO better mechanism for this
            columns = args.get('columns', df.columns.tolist())  # TODO better mechanism for this
            path = list(sorted(glob.glob(f"./{dataset}_*.parquet")))
            train_set = RayDMatrix(
                path,
                label=target,
                columns=columns,
                enable_categorical=is_categorical,
                filetype=RayFileType.PARQUET)

        # TODO: encoding for all non-numerical columns`

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
                "objective": loss,
                # "eval_metric": ["logloss", "error"],  # TODO: add this back
                # "eta": tune.loguniform(1e-4, 1e-1),  # TODO: pass tune with_parameters
                # "subsample": tune.uniform(0.5, 1.0),
                # "max_depth": tune.randint(1, 9)
            }

        if is_categorical:
            config["num_class"] = n_class

        # Other param settings
        model_args['ray_params_dict'] = {
            'num_actors': args.get('num_actors', 1),
            'cpus_per_actor': args.get('num_cpus_per_actor', 4),
            'elastic_training': True,
            'max_failed_actors': args.get('max_failed_actors', 2),
            'max_actor_restarts': 3,
        }

        ray_params = RayParams(**model_args['ray_params_dict'])
        evals_result = {}
        start_ts = datetime.now()

        bst = train(
            config,
            train_set,
            evals_result=evals_result,
            evals=[(train_set, "train")],
            verbose_eval=False,
            ray_params=ray_params)

        model_args["runtime"] = (datetime.now() - start_ts).total_seconds()
        self.model_storage.file_set('model', dill.dumps(bst))
        self.model_storage.json_set("model_args", model_args)
        ray.shutdown()

    def predict(self, df, args={}):
        model_args = self.model_storage.json_get("model_args")
        model = dill.loads(self.model_storage.file_get('model'))

        # preprocessing
        df = df.dropna()
        type_map = {
            dtype.integer: int,
            dtype.float: float,
            dtype.binary: int,
            dtype.categorical: int
        }
        for col, it in model_args['dtypes'].items():
            if col in df.columns:
                if it in (dtype.categorical, dtype.binary):
                    df[col] = pd.factorize(df[col])[0]
                df[col] = df[col].astype(type_map[it])

        df = df[model_args['input_cols']]
        dpred = RayDMatrix(df)  # , label=model_args['target'])

        predictions = predict(model, dpred, model_args['ray_params_dict'])
        predictions = pd.DataFrame(predictions, columns=[model_args['target']])
        predictions[f'{model_args["target"]}_explain'] = None
        return predictions
