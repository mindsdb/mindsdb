from mindsdb.libs.constants.mindsdb import *

from ludwig import LudwigModel
import pandas as pd


# @TODO: Define generci interface, similar to 'base_module' in the phases
class LudwigBackend():

    def __init__(self, transaction):
        self.transaction = transaction

    def _create_ludwig_dataframe(self, mode):
            if mode == 'train':
                indexes = self.transaction.input_data.train_indexes['ALL_ROWS_NO_GROUP_BY']
            elif mode == 'predict':
                indexes = self.transaction.input_data.all_indexes['ALL_ROWS_NO_GROUP_BY']
            else:
                raise Exception(f'Unknown mode specified: "{mode}"')
            model_definition = {'input_features': [], 'output_features': []}
            data = {}

            for col_ind, col in enumerate(self.transaction.persistent_model_metadata.columns):
                data[col] = []
                for row_ind in indexes:
                    data[col].append(self.transaction.input_data.data_array[row_ind][col_ind])

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

            return pd.DataFrame(data=data), model_definition

    def train(self):
        training_dataframe, model_definition = self._create_ludwig_dataframe('train')

        model = LudwigModel(model_definition)

        train_stats = model.train(training_dataframe, model_name=self.transaction.metadata.model_name)

        self.transaction.persistent_model_metadata.ludwig_data = {'ludwig_save_path': model.model.weights_save_path.rstrip('/model_weights_progress') + '/model'}


    def predict(self):
        predict_dataframe = self._create_ludwig_dataframe('predict')
        model = LudwigModel.load(self.transaction.persistent_model_metadata.ludwig_data['ludwig_save_path'])
        predictions = model.predict(data_df=predict_dataframe)
        print(predictions)
