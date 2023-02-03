import os
import sys
import json
import base64
import datetime as dt
from copy import deepcopy

import pandas as pd
from dateutil.parser import parse as parse_datetime

from sqlalchemy import func, null
import numpy as np

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.utilities.config import Config
from mindsdb.utilities.json_encoder import json_serialiser
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.interfaces.model.functions import (
    get_model_record,
    get_model_records
)
from mindsdb.interfaces.storage.json import get_json_storage
from mindsdb.interfaces.storage.model_fs import ModelStorage
from mindsdb.utilities.context import context as ctx

IS_PY36 = sys.version_info[1] <= 6


class ModelController():
    config: Config
    fs_store: FsStore

    def __init__(self) -> None:
        self.config = Config()
        self.fs_store = FsStore()

    def get_model_data(self, name: str = None, predictor_record=None, ml_handler_name='lightwood') -> dict:
        if predictor_record is None:
            predictor_record = get_model_record(except_absent=True, name=name, ml_handler_name=ml_handler_name)

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
        data['id'] = predictor_record.id
        data['version'] = predictor_record.version

        json_storage = get_json_storage(
            resource_id=predictor_record.id
        )
        data['json_ai'] = json_storage.get('json_ai')

        if data.get('accuracies', None) is not None:
            if len(data['accuracies']) > 0:
                data['accuracy'] = float(np.mean(list(data['accuracies'].values())))
        return data

    def describe_model(self, session,  project_name, model_name, attribute):
        model_record = get_model_record(
            name=model_name,
            project_name=project_name,
            except_absent=True
        )
        integration_record = db.Integration.query.get(model_record.integration_id)

        ml_handler_base = session.integration_controller.get_handler(integration_record.name)

        ml_handler = ml_handler_base._get_ml_handler(model_record.id)
        if not hasattr(ml_handler, 'describe'):
            raise Exception("ML handler doesn't support description")

        return ml_handler.describe(attribute)

    def get_models(self, with_versions=False, ml_handler_name=None, integration_id=None,
                   project_name=None):
        models = []
        show_active = True if with_versions is False else None
        for predictor_record in get_model_records(active=show_active, ml_handler_name=ml_handler_name,
                                                  integration_id=integration_id, project_name=project_name):
            model_data = self.get_model_data(predictor_record=predictor_record)
            reduced_model_data = {}

            for k in ['id', 'name', 'version', 'is_active', 'predict', 'status',
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

    def delete_model(self, model_name: str, project_name: str = 'mindsdb'):
        from mindsdb.interfaces.database.database import DatabaseController

        project_record = db.Project.query.filter(
            (func.lower(db.Project.name) == func.lower(project_name))
            & (db.Project.company_id == ctx.company_id)
            & (db.Project.deleted_at == null())
        ).first()
        if project_record is None:
            raise Exception(f"Project '{project_name}' does not exists")

        database_controller = DatabaseController()

        project = database_controller.get_project(project_name)

        predictors_records = get_model_records(
            name=model_name,
            project_id=project.id,
            active=None,
        )
        if len(predictors_records) == 0:
            raise Exception(f"Model '{model_name}' does not exist")

        is_cloud = self.config.get('cloud', False)
        if is_cloud:
            for predictor_record in predictors_records:
                model_data = self.get_model_data(predictor_record=predictor_record)
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
            modelStorage = ModelStorage(predictor_record.id)
            modelStorage.delete()
        db.session.commit()

    def rename_model(self, old_name, new_name):
        model_record = get_model_record(name=new_name)
        if model_record is None:
            raise Exception(f"Model with name '{new_name}' already exists")

        for model_record in get_model_records(name=old_name):
            model_record.name = new_name
        db.session.commit()

    def export_predictor(self, name: str) -> json:
        predictor_record = get_model_record(name=name, except_absent=True)

        fs_name = f'predictor_{ctx.company_id}_{predictor_record.id}'
        self.fs_store.pull()
        local_predictor_savefile = os.path.join(self.fs_store.folder_path, fs_name)
        predictor_binary = open(local_predictor_savefile, 'rb').read()

        # Serialize a predictor record into a dictionary 
        # move into the Predictor db class itself if we use it again somewhere
        json_storage = get_json_storage(
            resource_id=predictor_record.id
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

    def import_predictor(self, name: str, payload: json) -> None:
        prs = json.loads(payload)

        predictor_record = db.Predictor(
            name=name,
            data=prs['data'],
            to_predict=prs['to_predict'],
            company_id=ctx.company_id,
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
        fs_name = f'predictor_{ctx.company_id}_{predictor_record.id}'
        with open(os.path.join(self.fs_store.folder_path, fs_name), 'wb') as fp:
            fp.write(predictor_binary)

        self.fs_store.push()

    @staticmethod
    def _get_data_integration_ref(statement, database_controller):
        # TODO use database_controller handler_controller internally
        data_integration_ref = None
        fetch_data_query = None
        if statement.integration_name is not None:
            fetch_data_query = statement.query_str
            integration_name = statement.integration_name.parts[0]

            databases_meta = database_controller.get_dict()
            data_integration_meta = databases_meta[integration_name]
            # TODO improve here. Suppose that it is view
            if data_integration_meta['type'] == 'project':
                data_integration_ref = {
                    'type': 'view'
                }
            else:
                data_integration_ref = {
                    'type': 'integration',
                    'id': data_integration_meta['id']
                }
        return data_integration_ref, fetch_data_query

    def prepare_create_statement(self, statement, database_controller, handler_controller):
        # extract data from Create model or Retrain statement and prepare it for using in crate and retrain functions
        project_name = statement.name.parts[0].lower()
        model_name = statement.name.parts[1].lower()

        problem_definition = {}
        if statement.targets is not None:
            problem_definition['target'] = statement.targets[0].parts[-1]

        data_integration_ref, fetch_data_query = self._get_data_integration_ref(statement, database_controller)

        label = None
        if statement.using is not None:
            label = statement.using.pop('tag', None)

            problem_definition['using'] = statement.using

        if statement.order_by is not None:
            problem_definition['timeseries_settings'] = {
                'is_timeseries': True,
                'order_by': getattr(statement, 'order_by')[0].field.parts[-1]
            }
            for attr in ['horizon', 'window']:
                if getattr(statement, attr) is not None:
                    problem_definition['timeseries_settings'][attr] = getattr(statement, attr)

            if statement.group_by is not None:
                problem_definition['timeseries_settings']['group_by'] = [col.parts[-1] for col in statement.group_by]

        join_learn_process = False
        if 'join_learn_process' in problem_definition.get('using', {}):
            join_learn_process = problem_definition['using']['join_learn_process']
            del problem_definition['using']['join_learn_process']

        return dict(
            model_name=model_name,
            project_name=project_name,
            data_integration_ref=data_integration_ref,
            fetch_data_query=fetch_data_query,
            problem_definition=problem_definition,
            join_learn_process=join_learn_process,
            label=label
        )

    def create_model(self, statement, ml_handler):
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

        predictor_record = ml_handler.learn(**params)

        return self.get_model_info(predictor_record)

    def retrain_model(self, statement, ml_handler):
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
            active=True
        )

        model_name = params['model_name']
        if base_predictor_record is None:
            raise Exception(f"Error: model '{model_name}' does not exist")

        params['version'] = self._get_retrain_adjust_version(model_name, params['project_name'], base_predictor_record)

        if params['data_integration_ref'] is None:
            params['data_integration_ref'] = base_predictor_record.data_integration_ref
        if params['fetch_data_query'] is None:
            params['fetch_data_query'] = base_predictor_record.fetch_data_query

        problem_definition = base_predictor_record.learn_args.copy()
        problem_definition.update(params['problem_definition'])
        params['problem_definition'] = problem_definition

        params['is_retrain'] = True
        params['set_active'] = set_active
        predictor_record = ml_handler.learn(**params)

        return self.get_model_info(predictor_record)

    @staticmethod
    def _get_retrain_adjust_version(model_name, project_name, base_predictor_record):
        if base_predictor_record is None:
            raise Exception(f"Error: model '{model_name}' does not exist")

        # get max current version
        models = get_model_records(
            name=model_name,
            project_name=project_name,
            deleted_at=None,
            active=None,
        )
        last_version = 1
        for m in models:
            if m.version is not None:
                last_version = max(last_version, m.version)

        return last_version + 1

    def prepare_adjust_statement(self, statement, database_controller):
        project_name = statement.name.parts[0].lower()
        model_name = statement.name.parts[1].lower()
        data_integration_ref, fetch_data_query = self._get_data_integration_ref(statement, database_controller)

        label = None
        args = {}
        if statement.using is not None:
            label = statement.using.pop('tag', None)
            args = statement.using

        join_learn_process = args.pop('join_learn_process', False)

        base_predictor_record = get_model_record(
            name=model_name,
            project_name=project_name,
            active=True
        )
        version = self._get_retrain_adjust_version(model_name, project_name, base_predictor_record)

        if data_integration_ref is None:
            data_integration_ref = base_predictor_record.data_integration_ref
        if fetch_data_query is None:
            fetch_data_query = base_predictor_record.fetch_data_query

        return dict(
            model_name=model_name,
            project_name=project_name,
            data_integration_ref=data_integration_ref,
            fetch_data_query=fetch_data_query,
            version=version,
            args=args,
            join_learn_process=join_learn_process,
            label=label
        )

    def adjust_model(self, statement, ml_handler):
        # active setting
        set_active = True
        if statement.using is not None:
            set_active = statement.using.pop('active', True)
            if set_active in ('0', 0, None):
                set_active = False

        params = self.prepare_adjust_statement(statement, ml_handler.database_controller)

        params['set_active'] = set_active
        predictor_record = ml_handler.update(**params)
        return self.get_model_info(predictor_record)

    def get_model_info(self, predictor_record):

        from mindsdb.interfaces.database.projects import ProjectController
        projects_controller = ProjectController()
        project = projects_controller.get(id=predictor_record.project_id)

        columns = ['NAME', 'ENGINE', 'PROJECT', 'ACTIVE', 'VERSION', 'STATUS', 'ACCURACY', 'PREDICT', 'UPDATE_STATUS',
                   'MINDSDB_VERSION', 'ERROR', 'SELECT_DATA_QUERY', 'TRAINING_OPTIONS', 'TAG']

        project_name = project.name
        model = project.get_models(model_id=predictor_record.id)[0]
        table_name = model['name']
        table_meta = model['metadata']
        record = [
            table_name, table_meta['engine'], project_name, table_meta['active'], table_meta['version'], table_meta['status'],
            table_meta['accuracy'], table_meta['predict'], table_meta['update_status'],
            table_meta['mindsdb_version'], table_meta['error'], table_meta['select_data_query'],
            str(table_meta['training_options']), table_meta['label']
        ]

        return pd.DataFrame([record], columns=columns)

    def update_model_version(self, models, active=None):
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
            db.Predictor.company_id == ctx.company_id,
            db.Predictor.id != model_record.id
        )
        for p in model_records:
            p.active = False

        db.session.commit()

    def delete_model_version(self, models):
        if len(models) == 0:
            raise Exception(f"Version to delete is not found")

        for model in models:
            model_record = get_model_record(
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
            modelStorage = ModelStorage(model_record.id)
            modelStorage.delete()

        db.session.commit()

