from typing import Optional

import dill
import dask
import pandas as pd
from ludwig.automl import auto_train

from mindsdb.integrations.libs.base import BaseMLEngine
from .utils import RayConnection


class LudwigHandler(BaseMLEngine):
    """
    Integration with the Ludwig declarative ML library.
    """  # noqa

    name = 'ludwig'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        args = args['using']  # ignore the rest of the problem definition

        # TODO: filter out incompatible use cases (e.g. time series won't work currently)
        # TODO: enable custom values via `args` (mindful of local vs cloud)
        user_config = {'hyperopt': {'executor': {'gpu_resources_per_trial': 0, 'num_samples': 3}}}  # no GPU for now

        with RayConnection():
            results = auto_train(
                dataset=df,
                target=target,
                tune_for_memory=False,
                time_limit_s=120,
                user_config=user_config,
                # output_directory='./',
                # random_seed=42,
                # use_reference_config=False,
                # kwargs={}
            )
        model = results.best_model
        args['dtype_dict'] = {f['name']: f['type'] for f in model.base_config['input_features']}
        args['accuracies'] = {'metric': results.experiment_analysis.best_result['metric_score']}
        self.model_storage.json_set('args', args)
        self.model_storage.file_set('model', dill.dumps(model))

    def predict(self, df, args=None):
        model = dill.loads(self.model_storage.file_get('model'))
        with RayConnection():
            predictions = self._call_model(df, model)
        return predictions

    @staticmethod
    def _call_model(df, model):
        predictions = dask.compute(model.predict(df)[0])[0]
        target_name = model.config['output_features'][0]['column']

        if target_name not in df:
            predictions.columns = [target_name]
        else:
            predictions.columns = ['prediction']

        predictions[f'{target_name}_explain'] = None
        joined = df.join(predictions)

        if 'prediction' in joined:
            joined = joined.rename({
                target_name: f'{target_name}_original',
                'prediction': target_name
            }, axis=1)
        return joined
