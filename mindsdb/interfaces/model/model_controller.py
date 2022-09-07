import os
import sys
import json
import base64
from copy import deepcopy
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_datetime

import numpy as np

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.utilities.config import Config
from mindsdb.utilities.json_encoder import json_serialiser
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.interfaces.model.functions import (
    get_model_record,
    get_model_records
)

IS_PY36 = sys.version_info[1] <= 6


class ModelController():
    config: Config
    fs_store: FsStore

    def __init__(self) -> None:
        self.config = Config()
        self.fs_store = FsStore()

    def get_model_data(self, company_id: int, name: str = None, predictor_record=None) -> dict:
        if predictor_record is None:
            predictor_record = get_model_record(company_id=company_id, except_absent=True, name=name)

        data = deepcopy(predictor_record.data)
        data['dtype_dict'] = predictor_record.dtype_dict
        data['created_at'] = str(parse_datetime(str(predictor_record.created_at).split('.')[0]))
        data['updated_at'] = str(parse_datetime(str(predictor_record.updated_at).split('.')[0]))
        data['training_start_at'] = predictor_record.training_start_at
        data['training_stop_at'] = predictor_record.training_stop_at
        data['predict'] = predictor_record.to_predict[0]
        data['update'] = predictor_record.update_status
        data['mindsdb_version'] = predictor_record.mindsdb_version
        data['name'] = predictor_record.name
        data['code'] = predictor_record.code
        data['json_ai'] = predictor_record.json_ai
        data['problem_definition'] = predictor_record.learn_args
        data['fetch_data_query'] = predictor_record.fetch_data_query
        data['active'] = predictor_record.active

        # assume older models are complete, only temporary
        if 'status' in predictor_record.data:
            data['status'] = predictor_record.data['status']
        elif 'error' in predictor_record.data:
            data['status'] = 'error'
        elif predictor_record.update_status == 'available':
            data['status'] = 'complete'
        elif predictor_record.json_ai is None and predictor_record.code is None:
            data['status'] = 'generating'
        elif predictor_record.data is None:
            data['status'] = 'editable'
        elif 'training_log' in predictor_record.data:
            data['status'] = 'training'
        elif 'error' not in predictor_record.data:
            data['status'] = 'complete'
        else:
            data['status'] = 'error'

        if data.get('accuracies', None) is not None:
            if len(data['accuracies']) > 0:
                data['accuracy'] = float(np.mean(list(data['accuracies'].values())))
        return data

    def get_model_description(self, name: str, company_id: int):
        """
        Similar to `get_model_data` but meant to be seen directly by the user, rather than parsed by something like the Studio predictor view.

        Uses `get_model_data` to compose this, but in the future we might want to make this independent if we deprecated `get_model_data`

        :returns: Dictionary of the analysis (meant to be foramtted by the APIs and displayed as json/yml/whatever)
        """ # noqa
        model_description = {}
        model_data = self.get_model_data(name=name, company_id=company_id)

        model_description['accuracies'] = model_data['accuracies']
        model_description['column_importances'] = model_data['column_importances']
        model_description['outputs'] = [model_data['predict']]
        model_description['inputs'] = [col for col in model_data['dtype_dict'] if col not in model_description['outputs']]
        model_description['model'] = ' --> '.join(str(k) for k in model_data['json_ai'])

        return model_description

    def get_models(self, company_id: int, with_versions=False):
        models = []
        show_active = True if with_versions is False else None
        for predictor_record in get_model_records(company_id=company_id, active=show_active):
            model_data = self.get_model_data(predictor_record=predictor_record, company_id=company_id)
            reduced_model_data = {}

            for k in ['name', 'version', 'is_active', 'predict', 'status',
                      'current_phase', 'accuracy', 'data_source', 'update', 'active',
                      'mindsdb_version', 'error', 'created_at', 'fetch_data_query']:
                reduced_model_data[k] = model_data.get(k, None)

            reduced_model_data['training_time'] = None
            if model_data.get('training_start_at') is not None:
                if model_data.get('training_stop_at') is not None:
                    reduced_model_data['training_time'] = (
                        model_data.get('training_stop_at')
                        - model_data.get('training_start_at')
                    )
                elif model_data.get('status') == 'training':
                    reduced_model_data['training_time'] = (
                        datetime.now()
                        - model_data.get('training_start_at')
                    )
                if reduced_model_data['training_time'] is not None:
                    reduced_model_data['training_time'] = (
                        reduced_model_data['training_time']
                        - timedelta(microseconds=reduced_model_data['training_time'].microseconds)
                    )

            models.append(reduced_model_data)
        return models

    def delete_model(self, model_name: str, company_id: int, integration_name: str = 'lightwood'):
        if integration_name.lower() == 'mindsdb':
            integration_name = 'lightwood'
        integration_controller = WithKWArgsWrapper(IntegrationController(), company_id=company_id)
        ml_handler = integration_controller.get_handler(integration_name)
        response = ml_handler.native_query(f'drop predictor {model_name}')
        if response.type == RESPONSE_TYPE.ERROR:
            raise Exception(response.error_message)

    def rename_model(self, old_name, new_name, company_id: int):
        model_record = get_model_record(company_id=company_id, name=new_name)
        if model_record is None:
            raise Exception(f"Model with name '{new_name}' already exists")

        for model_record in get_model_records(company_id=company_id, name=old_name):
            model_record.name = new_name
        db.session.commit()

    def export_predictor(self, name: str, company_id: int) -> json:
        predictor_record = get_model_record(company_id=company_id, name=name, except_absent=True)

        fs_name = f'predictor_{company_id}_{predictor_record.id}'
        self.fs_store.get(fs_name, base_dir=self.config['paths']['predictors'])
        local_predictor_savefile = os.path.join(self.config['paths']['predictors'], fs_name)
        predictor_binary = open(local_predictor_savefile, 'rb').read()

        # Serialize a predictor record into a dictionary 
        # move into the Predictor db class itself if we use it again somewhere
        predictor_record_serialized = {
            'name': predictor_record.name,
            'data': predictor_record.data,
            'to_predict': predictor_record.to_predict,
            'mindsdb_version': predictor_record.mindsdb_version,
            'native_version': predictor_record.native_version,
            'is_custom': predictor_record.is_custom,
            'learn_args': predictor_record.learn_args,
            'update_status': predictor_record.update_status,
            'json_ai': predictor_record.json_ai,
            'code': predictor_record.code,
            'lightwood_version': predictor_record.lightwood_version,
            'dtype_dict': predictor_record.dtype_dict,
            'predictor_binary': predictor_binary
        }

        return json.dumps(predictor_record_serialized, default=json_serialiser)

    def import_predictor(self, name: str, payload: json, company_id: int) -> None:
        prs = json.loads(payload)

        predictor_record = db.Predictor(
            name=name,
            data=prs['data'],
            to_predict=prs['to_predict'],
            company_id=company_id,
            mindsdb_version=prs['mindsdb_version'],
            native_version=prs['native_version'],
            is_custom=prs['is_custom'],
            learn_args=prs['learn_args'],
            update_status=prs['update_status'],
            json_ai=prs['json_ai'],
            code=prs['code'],
            lightwood_version=prs['lightwood_version'],
            dtype_dict=prs['dtype_dict']
        )

        db.session.add(predictor_record)
        db.session.commit()

        predictor_binary = base64.b64decode(prs['predictor_binary'])
        fs_name = f'predictor_{company_id}_{predictor_record.id}'
        with open(os.path.join(self.config['paths']['predictors'], fs_name), 'wb') as fp:
            fp.write(predictor_binary)

        self.fs_store.put(fs_name, base_dir=self.config['paths']['predictors'])
