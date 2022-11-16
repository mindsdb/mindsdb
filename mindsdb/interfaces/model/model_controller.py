import os
import sys
import json
import base64
import datetime as dt
from copy import deepcopy
from dateutil.parser import parse as parse_datetime

from sqlalchemy import func, null
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
from mindsdb.interfaces.storage.json import get_json_storage
from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage
from mindsdb.interfaces.database.database import DatabaseController

IS_PY36 = sys.version_info[1] <= 6


class ModelController():
    config: Config
    fs_store: FsStore

    def __init__(self) -> None:
        self.config = Config()
        self.fs_store = FsStore()

    def get_model_data(self, company_id: int, name: str = None, predictor_record=None, ml_handler_name='lightwood') -> dict:
        if predictor_record is None:
            predictor_record = get_model_record(company_id=company_id, except_absent=True, name=name, ml_handler_name=ml_handler_name)

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
        data['problem_definition'] = predictor_record.learn_args
        data['fetch_data_query'] = predictor_record.fetch_data_query
        data['active'] = predictor_record.active
        data['status'] = predictor_record.status

        json_storage = get_json_storage(
            resource_id=predictor_record.id,
            company_id=predictor_record.company_id
        )
        data['json_ai'] = json_storage.get('json_ai')

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

    def get_models(self, company_id: int, with_versions=False, ml_handler_name='lightwood', integration_id=None):
        models = []
        show_active = True if with_versions is False else None
        for predictor_record in get_model_records(company_id=company_id, active=show_active, ml_handler_name=ml_handler_name, integration_id=integration_id):
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
                        dt.datetime.now()
                        - model_data.get('training_start_at')
                    )
                if reduced_model_data['training_time'] is not None:
                    reduced_model_data['training_time'] = (
                        reduced_model_data['training_time']
                        - dt.timedelta(microseconds=reduced_model_data['training_time'].microseconds)
                    )

            models.append(reduced_model_data)
        return models

    def delete_model(self, model_name: str, company_id: int, project_name: str = 'mindsdb'):

        project_record = db.Project.query.filter(
            (func.lower(db.Project.name) == func.lower(project_name))
            & (db.Project.company_id == company_id)
            & (db.Project.deleted_at == null())
        ).first()
        if project_record is None:
            raise Exception(f"Project '{project_name}' does not exists")

        model_record = db.Predictor.query.filter(
            func.lower(db.Predictor.name) == func.lower(model_name),
            db.Predictor.project_id == project_record.id,
            db.Predictor.company_id == company_id
        ).first()
        if model_record is None:
            raise Exception(f"Model '{model_name}' does not exists")

        integration_record = db.Integration.query.get(model_record.integration_id)
        if integration_record is None:
            raise Exception(f"Can't determine integration of '{model_name}'")

        database_controller = WithKWArgsWrapper(
            DatabaseController(),
            company_id=company_id
        )

        project = database_controller.get_project(project_name)

        predictors_records = get_model_records(
            company_id=company_id,
            name=model_name,
            ml_handler_name=integration_record.name,
            project_id=project.id,
            active=None,
        )
        if len(predictors_records) == 0:
            raise Exception(f"Model '{model_name}' does not exist")

        is_cloud = self.config.get('cloud', False)
        if is_cloud:
            for predictor_record in predictors_records:
                model_data = self.get_model_data(predictor_record=predictor_record, company_id=company_id)
                if (
                    model_data.get('status') in ['generating', 'training']
                    and isinstance(model_data.get('created_at'), str) is True
                    and (dt.datetime.now() - parse_datetime(model_data.get('created_at'))) < dt.timedelta(hours=1)
                ):
                    raise Exception('You are unable to delete models currently in progress, please wait before trying again')

        for predictor_record in predictors_records:
            if is_cloud:
                predictor_record.deleted_at = dt.datetime.now()
            else:
                db.session.delete(predictor_record)
            modelStorage = ModelStorage(company_id, predictor_record.id)
            modelStorage.delete()
        db.session.commit()

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
        self.fs_store.pull()
        local_predictor_savefile = os.path.join(self.fs_store.folder_path, fs_name)
        predictor_binary = open(local_predictor_savefile, 'rb').read()

        # Serialize a predictor record into a dictionary 
        # move into the Predictor db class itself if we use it again somewhere
        json_storage = get_json_storage(
            resource_id=predictor_record.id,
            company_id=predictor_record.company_id
        )
        predictor_record_serialized = {
            'name': predictor_record.name,
            'data': predictor_record.data,
            'to_predict': predictor_record.to_predict,
            'mindsdb_version': predictor_record.mindsdb_version,
            'native_version': predictor_record.native_version,
            'is_custom': predictor_record.is_custom,
            'learn_args': predictor_record.learn_args,
            'update_status': predictor_record.update_status,
            'json_ai': json_storage.get('json_ai'),
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
        with open(os.path.join(self.fs_store.folder_path, fs_name), 'wb') as fp:
            fp.write(predictor_binary)

        self.fs_store.push()

    def prepare_create_statement(self, statement, database_controller, handler_controller):
        # extract data from Create model or Retrain statement and prepare it for using in crate and retrain functions

        # TODO use database_controller handler_controller internally

        project_name = statement.name.parts[0].lower()
        model_name = statement.name.parts[1].lower()

        problem_definition = {}
        if statement.targets is not None:
            problem_definition['target'] = statement.targets[0].parts[-1]

        # get data for learn
        data_integration_id = None
        fetch_data_query = None
        if statement.integration_name is not None:
            fetch_data_query = statement.query_str
            integration_name = statement.integration_name.parts[0]

            databases_meta = database_controller.get_dict()
            data_integration_meta = databases_meta[integration_name]
            # TODO improve here. Suppose that it is view
            if data_integration_meta['type'] == 'project':
                data_integration_id = handler_controller.get(name='views')['id']
            else:
                data_integration_id = data_integration_meta['id']

        label = None
        if statement.using is not None:
            label = statement.using.pop('tag', None)

            problem_definition['using'] = statement.using

        if statement.order_by is not None:
            problem_definition['timeseries_settings'] = {
                'is_timeseries': True,
                'order_by': str(getattr(statement, 'order_by')[0])
            }
            for attr in ['horizon', 'window']:
                if getattr(statement, attr) is not None:
                    problem_definition['timeseries_settings'][attr] = getattr(statement, attr)

            if statement.group_by is not None:
                problem_definition['timeseries_settings']['group_by'] = [str(col) for col in statement.group_by]

        join_learn_process = False
        if 'join_learn_process' in problem_definition.get('using', {}):
            join_learn_process = problem_definition['using']['join_learn_process']
            del problem_definition['using']['join_learn_process']

        return dict(
            model_name=model_name,
            project_name=project_name,
            data_integration_id=data_integration_id,
            fetch_data_query=fetch_data_query,
            problem_definition=problem_definition,
            join_learn_process=join_learn_process,
            label=label
        )

    def create_model(self, statement, ml_handler, company_id: int):
        params = self.prepare_create_statement(statement,
                                               ml_handler.database_controller,
                                               ml_handler.handler_controller)

        existing_projects_meta = ml_handler.database_controller.get_dict(filter_type='project')
        if params['project_name'] not in existing_projects_meta:
            raise Exception(f"Project '{params['project_name']}' does not exist.")

        project = ml_handler.database_controller.get_project(name=params['project_name'])
        project_tables = project.get_tables()
        if params['model_name'] in project_tables:
            raise Exception(f"Error: model '{params['model_name']}' already exists in project {params['project_name']}!")

        ml_handler.learn(**params)

    def retrain_model(self, statement, ml_handler, company_id: int):
        # active setting
        set_active = True
        if statement.using is not None:
            set_active = statement.using.pop('active', True)
            if set_active in ('0', 0, None):
                set_active = False

        params = self.prepare_create_statement(statement,
                                               ml_handler.database_controller,
                                               ml_handler.handler_controller)

        base_predictor_record = get_model_record(
            name=params['model_name'],
            project_name=params['project_name'],
            company_id=ml_handler.company_id,
            active=True
        )

        model_name = params['model_name']
        if base_predictor_record is None:
            raise Exception(f"Error: model '{model_name}' does not exists")

        # get max current version
        models = get_model_records(
            name=params['model_name'],
            project_name=params['project_name'],
            company_id=ml_handler.company_id,
            deleted_at=None,
            active=None,
        )
        version0 = max([m.version for m in models])
        if version0 is None:
            version0 = 1

        params['version'] = version0 + 1

        # get params from predictor if not defined

        if params['data_integration_id'] is None:
            params['data_integration_id'] = base_predictor_record.data_integration_id
        if params['fetch_data_query'] is None:
            params['fetch_data_query'] = base_predictor_record.fetch_data_query

        problem_definition = base_predictor_record.learn_args.copy()
        problem_definition.update(params['problem_definition'])
        params['problem_definition'] = problem_definition

        params['is_retrain'] = True
        params['set_active'] = set_active
        ml_handler.learn(**params)

    def update_model_version(self, models, company_id: int, active=None):
        if active is None:
            raise NotImplementedError(f'Update is not supported')

        if active in ('0', 0, False):
            active = False
        else:
            active = True

        if active is False:
            raise NotImplementedError('Only setting active version is possible')

        if len(models) != 1:
            raise Exception('Only one version can be updated')

        # update
        model = models[0]

        model_record = get_model_record(
            company_id=company_id,
            name=model['NAME'],
            project_name=model['PROJECT'],
            version=model['VERSION']
        )

        model_record.active = True

        # deactivate current active version
        model_records = db.Predictor.query.filter(
            db.Predictor.name == model_record.name,
            db.Predictor.project_id == model_record.project_id,
            db.Predictor.active == True,
            db.Predictor.company_id == company_id,
            db.Predictor.id != model_record.id
        )
        for p in model_records:
            p.active = False

        db.session.commit()

    def delete_model_version(self, models, company_id: int):
        if len(models) == 0:
            raise Exception(f"Version to delete is not found")

        for model in models:
            model_record = get_model_record(
                company_id=company_id,
                name=model['NAME'],
                project_name=model['PROJECT'],
                version=model['VERSION']
            )
            if model_record.active:
                raise Exception(f"Can't remove active version: f{model['PROJECT']}.{model['NAME']}.{model['VERSION']}")

            is_cloud = self.config.get('cloud', False)
            if is_cloud:
                model_record.deleted_at = dt.datetime.now()
            else:
                db.session.delete(model_record)
            modelStorage = ModelStorage(company_id, model_record.id)
            modelStorage.delete()

        db.session.commit()

