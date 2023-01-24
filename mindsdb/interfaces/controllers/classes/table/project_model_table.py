from copy import deepcopy

import numpy as np

from mindsdb.interfaces.controllers.abc.table import Table


class ProjectModelTable(Table):
    type = 'model'
    mysql_type = 'BASE TABLE'
    mindsdb_type = 'MODEL'

    @staticmethod
    def from_record(model_record, integration_record):
        model = ProjectModelTable()
        model.name = model_record.name
        model.id = model_record.id
        model.engine = integration_record.name

        predictor_data = deepcopy(model_record.data) or {}
        predictor_meta = {
            'type': 'model',
            'id': model_record.id,
            'engine': integration_record.engine,
            'engine_name': integration_record.name,
            'active': model_record.active,
            'version': model_record.version,
            'status': model_record.status,
            'accuracy': None,
            'predict': model_record.to_predict[0],
            'update_status': model_record.update_status,
            'mindsdb_version': model_record.mindsdb_version,
            'error': predictor_data.get('error'),
            'select_data_query': model_record.fetch_data_query,
            'training_options': model_record.learn_args,
            'deletable': True,
            'label': model_record.label,
        }
        if predictor_data is not None and predictor_data.get('accuracies', None) is not None:
            if len(predictor_data['accuracies']) > 0:
                predictor_meta['accuracy'] = float(np.mean(list(predictor_data['accuracies'].values())))

        model.metadata = predictor_meta
        return model

    def get_columns(self):
        # TODO
        return []
