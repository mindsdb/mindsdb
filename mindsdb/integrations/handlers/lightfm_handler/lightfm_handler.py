from typing import Optional, Dict

import dill
import pandas as pd
from lightfm import LightFM
from mindsdb.integrations.handlers.lightfm_handler.helpers import RecommenderPreprocessor, get_item_item_recommendations, \
    get_user_item_recommendations, ModelParameters
from mindsdb.integrations.libs.base import BaseMLEngine


class LightFMHandler(BaseMLEngine):
    """
    Integration with the lightfm Recommender library.
    """

    name = 'lightfm'

    # todo change for hybrid recommender
    def create(self, target: str, df: pd.DataFrame = None, args: Optional[Dict] = None):

        args = args["using"]

        # get model parameters if defined by user - else use default values

        user_defined_model_params = list(filter(lambda x: x in args, ["learning_rate", "loss", "epochs"]))
        args['model_params'] = {model_param: args[model_param] for model_param in user_defined_model_params}
        model_parameters = ModelParameters(**args['model_params'])

        #
        rec_preprocessor = RecommenderPreprocessor(
            interaction_data=df,
            user_id_column_name=args['user_id'],
            item_id_column_name=args['item_id'],
            threshold=args['threshold'],
        )

        # preprocess data
        preprocessed_data = rec_preprocessor.preprocess()

        # get idx to item_id and user_id maps
        args['idx_to_item_id_map'] = preprocessed_data.idx_item_map
        args['idx_to_user_id_map'] = preprocessed_data.idx_user_map

        # todo train/test split

        # train model

        model = LightFM(learning_rate=model_parameters.learning_rate, loss=model_parameters.loss, random_state=42)
        model.fit(preprocessed_data.interaction_matrix, epochs=model_parameters.epochs)

        # todo evaluate model
        # todo check and return precision@k

        self.model_storage.file_set('model', dill.dumps(model))
        self.model_storage.json_set('args', args)

    # todo change for hybrid recommender
    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None):

        args = self.model_storage.json_get('args')
        model = dill.loads(self.model_storage.file_get('model'))

        n_users = df[args['user_id']].nunique()
        n_items = df[args['item_id']].nunique()

        if args['recommendation_type'] == 'item_item':
            return get_item_item_recommendations(model=model, args=args, item_features=None, N=args['n_recommendations'])

        elif args['recommendation_type'] == 'user_item':
            return get_user_item_recommendations(n_users=n_users, n_items=n_items, args=args, model=model)

        elif args['recommendation_type'] == 'user_user':
            raise NotImplementedError("user_user recommendation type is not implemented yet")

        else:
            raise ValueError("recommendation_type must be either 'user_item', 'item_item' or 'user_user'")
