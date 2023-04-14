from sklearn.metrics import r2_score
import dill
import pandas as pd
import tempfile
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.time_series_utils import (
    transform_to_nixtla_df,
    get_results_from_nixtla_df,
    infer_frequency,
    get_model_accuracy_dict,
    get_hierarchy_from_df,
    reconcile_forecasts
)
from neuralforecast import NeuralForecast
from neuralforecast.models import NHITS
from neuralforecast.auto import AutoNHITS
from ray.tune.search.hyperopt import HyperOptSearch

DEFAULT_FREQUENCY = "D"
DEFAULT_MODEL_NAME = "NHITS"
DEFAULT_TRIALS = 20
MIN_TRIALS_FOR_AUTOTUNE = 3


def choose_model(num_trials, horizon, window, exog_vars=[], threshold=MIN_TRIALS_FOR_AUTOTUNE):
    """Chooses model based on the number of trials allowed by the user.

    A lower args for training time reduces the number of trial models we can
    search through. Below a certain threshold, we switch to the default
    NHITS implementation instead of using AutoML to search for the best config.
    """
    if num_trials >= threshold:
        model = AutoNHITS(horizon, gpus=0, num_samples=num_trials, search_alg=HyperOptSearch())
    else:  # faster implementation without auto parameter tuning
        model = NHITS(horizon, window, hist_exog_list=exog_vars)
    return model


class NeuralForecastHandler(BaseMLEngine):
    """Integration with the Nixtla NeuralForecast library for
    time series forecasting with neural networks.
    """

    name = "neuralforecast"

    def create(self, target, df, args={}):
        """Create the NeuralForecast Handler.

        Requires specifying the target column to predict and time series arguments for
        prediction horizon, time column (order by) and grouping column(s).

        Saves model params to desk, which are called later in the predict() method.
        """
        time_settings = args["timeseries_settings"]
        using_args = args["using"]
        assert time_settings["is_timeseries"], "Specify time series settings in your query"
        ###### store model args and time series settings in the model folder
        model_args = {}
        model_args["target"] = target
        model_args["horizon"] = time_settings["horizon"]
        model_args["order_by"] = time_settings["order_by"]
        model_args["group_by"] = time_settings["group_by"]
        model_args["frequency"] = (
            using_args["frequency"] if "frequency" in using_args else infer_frequency(df, time_settings["order_by"])
        )
        model_args["model_name"] = DEFAULT_MODEL_NAME
        num_trials = int(DEFAULT_TRIALS * using_args["train_time"]) if "train_time" in using_args else DEFAULT_TRIALS
        model_args["exog_vars"] = using_args["exogenous_vars"] if "exogenous_vars" in using_args else []
        model_args["model_folder"] = tempfile.mkdtemp()

        # Deal with hierarchy
        model_args["hierarchy"] = using_args["hierarchy"] if "hierarchy" in using_args else False
        if model_args["hierarchy"]:
            training_df, hier_df, hier_dict = get_hierarchy_from_df(df, model_args)
            self.model_storage.file_set("hier_dict", dill.dumps(hier_dict))
            self.model_storage.file_set("hier_df", dill.dumps(hier_df))
            self.model_storage.file_set("training_df", dill.dumps(training_df))
        else:
            training_df = transform_to_nixtla_df(df, model_args, model_args["exog_vars"])

        # Train model
        model = choose_model(num_trials, time_settings["horizon"], time_settings["window"])
        neural = NeuralForecast(models=[model], freq=model_args["frequency"])
        results_df = neural.cross_validation(training_df)
        # Get model accuracy
        model_args["accuracies"] = get_model_accuracy_dict(results_df, r2_score)

        ###### persist changes to handler folder
        neural.save(model_args["model_folder"], overwrite=True)
        self.model_storage.json_set("model_args", model_args)

    def predict(self, df, args={}):
        """Makes forecasts with the NeuralForecast Handler.

        NeuralForecast is setup to predict for all groups, so it won't handle
        a dataframe that's been filtered to one group very well. Instead, we make
        the prediction for all groups then take care of the filtering after the
        forecasting. Prediction is nearly instant.
        """
        # Load model arguments
        model_args = self.model_storage.json_get("model_args")

        prediction_df = transform_to_nixtla_df(df, model_args)
        groups_to_keep = prediction_df["unique_id"].unique()

        neural = NeuralForecast.load(model_args["model_folder"])
        forecast_df = neural.predict()
        if model_args["hierarchy"]:
            training_df = dill.loads(self.model_storage.file_get("training_df"))
            hier_df = dill.loads(self.model_storage.file_get("hier_df"))
            hier_dict = dill.loads(self.model_storage.file_get("hier_dict"))
            reconciled_df = reconcile_forecasts(training_df, forecast_df, hier_df, hier_dict)
            results_df = reconciled_df[reconciled_df.index.isin(groups_to_keep)]
        else:
            results_df = forecast_df[forecast_df.index.isin(groups_to_keep)]
        return get_results_from_nixtla_df(results_df, model_args)

    def describe(self, attribute=None):
        model_args = self.model_storage.json_get("model_args")

        if attribute == "model":
            return pd.DataFrame({k: [model_args[k]] for k in ["model_name", "frequency", "hierarchy"]})

        elif attribute == "features":
            return pd.DataFrame(
                {"ds": [model_args["order_by"]], "y": model_args["target"], "unique_id": [model_args["group_by"]], "exog_vars": [model_args["exog_vars"]]}
            )

        elif attribute == 'info':
            outputs = model_args["target"]
            inputs = [model_args["target"], model_args["order_by"], model_args["group_by"]] + model_args["exog_vars"]
            accuracies = [(model, acc) for model, acc in model_args["accuracies"].items()]
            return pd.DataFrame({"accuracies": [accuracies], "outputs": outputs, "inputs": [inputs]})

        else:
            tables = ['info', 'features', 'model']
            return pd.DataFrame(tables, columns=['tables'])