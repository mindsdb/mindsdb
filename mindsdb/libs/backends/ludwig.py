from mindsdb.libs.constants.mindsdb import *

from ludwig import LudwigModel
import pandas as pd


# @TODO: Define generci interface, similar to 'base_module' in the phases
class LudwigBackend():

    def __init__(self, transaction):
        self.transaction = transaction

    def train(self):
        model_definition = {'input_features': [], 'output_features': []}
        training_data = {}

        for col_ind, col in enumerate(self.transaction.persistent_model_metadata.columns):
            training_data[col] = []
            for row_ind in self.transaction.input_data.train_indexes['ALL_ROWS_NO_GROUP_BY']:
                training_data[col].append(self.transaction.input_data.data_array[row_ind][col_ind])

            col_stats = self.transaction.persistent_model_metadata.column_stats[col]
            data_type = col_stats[KEYS.DATA_TYPE]

            ludwig_dtype = 'bag'

            if data_type == DATA_TYPES.NUMERIC:
                ludwig_dtype = 'numerical'

            if data_type == DATA_TYPES.TEXT:
                ludwig_dtype = 'bag'

            if data_type == DATA_TYPES.DATE:
                ludwig_dtype = 'bag'

            if data_type == DATA_TYPES.CATEGORICAL:
                ludwig_dtype = 'category'

            if col not in self.transaction.persistent_model_metadata.predict_columns:
                model_definition['input_features'].append({
                    'name': col
                    ,'type': ludwig_dtype
                })
            else:
                model_definition['output_features'].append({
                    'name': col
                    ,'type': ludwig_dtype
                })


        training_dataframe = pd.DataFrame(data=training_data)
        model = LudwigModel(model_definition)

        train_stats = model.train(training_dataframe, model_name=self.transaction.metadata.model_name)

        self.transaction.persistent_ml_model_info.ludwig_save_path = model.model.weights_save_path.rstrip('/model_weights_progress')


    def predict(self):
        model = LudwigModel.load(self.transaction.persistent_ml_model_info.ludwig_save_path)
        pass
