from typing import Dict
import pandas as pd
from typing import Optional
import logging
import os
from mindsdb.integrations.libs.base import BaseMLEngine
from autogluon.tabular import TabularPredictor


class AutoGluonHandler(BaseMLEngine):
    name = 'autogluon'

    # creates and trains a model inside the engine registry.
    def create(self, target: str, df: Optional[pd.DataFrame] = None, args=None, **kwargs) -> None:
        if 'using' in args:
            args = args['using']
        if 'target' in args:
            target = args['target']

        store_path = os.environ.get('MINDSDB_STORAGE_DIR') or ''
        save_path = os.path.join(store_path, 'mindsdb-predict')  # specifies folder to store trained models
        self.model_storage.json_set("model_path", {'model_dir': save_path})
        predictor = TabularPredictor(label=target, path=save_path).fit(df)

        # ###### persist changes to handler folder
        # save data for Describe Model without attributes
        model_deets = pd.DataFrame({'best_model': predictor.get_model_best(),
                                    'eval_metric': predictor.eval_metric.name,
                                    'original_features': [predictor.original_features],
                                    'problem_type': predictor.problem_type})
        self.model_storage.json_set("model_details", model_deets.to_dict())

        # save data for Describe Model features
        feats = pd.DataFrame(columns=['column', 'type', 'role'])
        for feat in predictor.features():
            datatype = predictor.feature_metadata_in.get_feature_type_raw(feat)
            feats = feats.append({'column': feat, 'type': datatype, 'role': 'feature'}, ignore_index=True)
        feats = feats.append({'column': target, 'type': df.dtypes[target].name, 'role': 'target'}, ignore_index=True)
        self.model_storage.json_set("feature_info", feats.to_dict())
        # Save data for Describe model
        candidates = predictor.leaderboard(df, silent=True)
        self.model_storage.json_set("candidate_models", candidates.to_dict())
        # self.model_storage.file_set("trained_model", dill.dumps(predictor))

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:

        save_path = self.model_storage.json_get("model_path")['model_dir']
        predictor = TabularPredictor.load(path=save_path)
        # AutoGluon needs all the feature columns it was trained on, even if they are not present in the input.
        for feat in predictor.features():
            datatype = predictor.feature_metadata_in.get_feature_type_raw(feat)
            if feat not in df.columns:
                df[feat] = pd.Series(dtype=datatype)
        y_pred = predictor.predict(df)
        return pd.DataFrame(y_pred)

    def update(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        logging.debug('Update!')

    def _get_model_info(self):
        model_info = self.model_storage.json_get("candidate_models")
        return pd.DataFrame(model_info)

    def _get_features_info(self):
        # columns name, type, role
        features_info = self.model_storage.json_get("feature_info")
        return pd.DataFrame(features_info)

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        # displays the performance of the candidate models. For AutoGluon, its the leaderboard.
        if attribute is None:
            model_description = self.model_storage.json_get("model_details")
            return pd.DataFrame(model_description)
        else:
            if attribute == "model":
                # model statement displays the performance of the candidate models.
                return self._get_model_info()
            elif attribute == "features":
                # features statement displays how the model encoded the data before the training process.
                return self._get_features_info()

    def create_engine(self, connection_args: dict):
        logging.debug('Create engine!')
