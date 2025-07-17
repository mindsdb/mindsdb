import copy
import datetime as dt
from copy import deepcopy
from multiprocessing.pool import ThreadPool

import pandas as pd
from dateutil.parser import parse as parse_datetime

from sqlalchemy import func
import numpy as np

import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.config import Config
from mindsdb.interfaces.model.functions import (
    get_model_record,
    get_model_records,
    get_project_record
)
from mindsdb.interfaces.storage.json import get_json_storage
from mindsdb.interfaces.storage.model_fs import ModelStorage
from mindsdb.utilities.config import config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.functions import resolve_model_identifier
import mindsdb.utilities.profiler as profiler
from mindsdb.utilities.exception import EntityExistsError, EntityNotExistsError
from mindsdb.utilities import log

logger = log.getLogger(__name__)


default_project = config.get('default_project')


def delete_model_storage(model_id, ctx_dump):
    try:
        ctx.load(ctx_dump)
        modelStorage = ModelStorage(model_id)
        modelStorage.delete()
    except Exception as e:
        logger.error(f'Something went wrong during deleting storage of model {model_id}: {e}')


class ModelController():
    config: Config

    def __init__(self) -> None:
        self.config = Config()

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

    def get_reduced_model_data(self, name: str = None, predictor_record=None, ml_handler_name='lightwood') -> dict:
        full_model_data = self.get_model_data(name=name, predictor_record=predictor_record, ml_handler_name=ml_handler_name)
        reduced_model_data = {}
        for k in ['id', 'name', 'version', 'is_active', 'predict', 'status',
                  'problem_definition', 'current_phase', 'accuracy', 'data_source', 'update', 'active',
                  'mindsdb_version', 'error', 'created_at', 'fetch_data_query']:
            reduced_model_data[k] = full_model_data.get(k, None)

        reduced_model_data['training_time'] = None
        if full_model_data.get('training_start_at') is not None:
            if full_model_data.get('training_stop_at') is not None:
                reduced_model_data['training_time'] = (
                    full_model_data.get('training_stop_at')
                    - full_model_data.get('training_start_at')
                )
            elif full_model_data.get('status') == 'training':
                reduced_model_data['training_time'] = (
                    dt.datetime.now()
                    - full_model_data.get('training_start_at')
                )
            if reduced_model_data['training_time'] is not None:
                reduced_model_data['training_time'] = (
                    reduced_model_data['training_time']
                    - dt.timedelta(microseconds=reduced_model_data['training_time'].microseconds)
                ).total_seconds()

        return reduced_model_data

    def describe_model(self, session, project_name, model_name, attribute, version=None):
        args = {
            'name': model_name,
            'version': version,
            'project_name': project_name,
            'except_absent': True
        }
        if version is not None:
            args['active'] = None

        model_record = get_model_record(**args)

        integration_record = db.Integration.query.get(model_record.integration_id)

        ml_handler_base = session.integration_controller.get_ml_handler(integration_record.name)

        return ml_handler_base.describe(model_record.id, attribute)

    def get_model(self, name, version=None, ml_handler_name=None, project_name=None):
        show_active = True if version is None else None
        model_record = get_model_record(
            active=show_active,
            version=version,
            name=name,
            ml_handler_name=ml_handler_name,
            except_absent=True,
            project_name=project_name)
        data = self.get_reduced_model_data(predictor_record=model_record)
        integration_record = db.Integration.query.get(model_record.integration_id)
        if integration_record is not None:
            data['engine'] = integration_record.engine
            data['engine_name'] = integration_record.name
        return data

    def get_models(self, with_versions=False, ml_handler_name=None, integration_id=None,
                   project_name=None):
        models = []
        show_active = True if with_versions is False else None
        for model_record in get_model_records(active=show_active, ml_handler_name=ml_handler_name,
                                              integration_id=integration_id, project_name=project_name):
            model_data = self.get_reduced_model_data(predictor_record=model_record)
            models.append(model_data)
        return models

    def delete_model(self, model_name: str, project_name: str = default_project, version=None):
        from mindsdb.interfaces.database.database import DatabaseController

        project_record = get_project_record(func.lower(project_name))
        if project_record is None:
            raise Exception(f"Project '{project_name}' does not exists")

        database_controller = DatabaseController()

        project = database_controller.get_project(project_name)

        if version is None:
            # Delete latest version
            predictors_records = get_model_records(
                name=model_name,
                project_id=project.id,
                active=None,
            )
        else:
            predictors_records = get_model_records(
                name=model_name,
                project_id=project.id,
                version=version,
            )
        if len(predictors_records) == 0:
            raise EntityNotExistsError('Model does not exist', model_name)

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
        db.session.commit()

        # region delete storages
        if len(predictors_records) > 1:
            ctx_dump = ctx.dump()
            with ThreadPool(min(len(predictors_records), 100)) as pool:
                pool.starmap(delete_model_storage, [(record.id, ctx_dump) for record in predictors_records])
        else:
            modelStorage = ModelStorage(predictors_records[0].id)
            modelStorage.delete()
        # endregion

    def rename_model(self, old_name, new_name):
        model_record = get_model_record(name=new_name)
        if model_record is None:
            raise Exception(f"Model with name '{new_name}' already exists")

        for model_record in get_model_records(name=old_name):
            model_record.name = new_name
        db.session.commit()

    @staticmethod
    def _get_data_integration_ref(statement, database_controller):
        # TODO use database_controller handler_controller internally
        data_integration_ref = None
        fetch_data_query = None
        if statement.integration_name is not None:
            fetch_data_query = statement.query_str
            integration_name = statement.integration_name.parts[0].lower()

            databases_meta = database_controller.get_dict()
            if integration_name not in databases_meta:
                raise EntityNotExistsError('Database does not exist', integration_name)
            data_integration_meta = databases_meta[integration_name]
            # TODO improve here. Suppose that it is view
            if data_integration_meta['type'] == 'project':
                data_integration_ref = {
                    'type': 'project'
                }
            elif data_integration_meta['type'] == 'system':
                data_integration_ref = {
                    'type': 'system'
                }
            else:
                data_integration_ref = {
                    'type': 'integration',
                    'id': data_integration_meta['id']
                }
        return data_integration_ref, fetch_data_query

    def prepare_create_statement(self, statement, database_controller):
        # extract data from Create model or Retrain statement and prepare it for using in crate and retrain functions
        project_name = statement.name.parts[0].lower()
        model_name = statement.name.parts[1].lower()

        sql_task = None
        if statement.task is not None:
            sql_task = statement.task.to_string()
        problem_definition = {
            '__mdb_sql_task': sql_task
        }
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
        params = self.prepare_create_statement(statement, ml_handler.database_controller)

        existing_projects_meta = ml_handler.database_controller.get_dict(filter_type='project')
        if params['project_name'] not in existing_projects_meta:
            raise EntityNotExistsError('Project does not exist', params['project_name'])

        project = ml_handler.database_controller.get_project(name=params['project_name'])
        project_tables = project.get_tables()
        if params['model_name'] in project_tables:
            raise EntityExistsError('Model already exists', f"{params['project_name']}.{params['model_name']}")
        predictor_record = ml_handler.learn(**params)

        return ModelController.get_model_info(predictor_record)

    def retrain_model(self, statement, ml_handler):
        # active setting
        set_active = True
        if statement.using is not None:
            set_active = statement.using.pop('active', True)
            if set_active in ('0', 0, None):
                set_active = False

        params = self.prepare_create_statement(statement, ml_handler.database_controller)

        base_predictor_record = get_model_record(
            name=params['model_name'],
            project_name=params['project_name'],
            active=True
        )

        model_name = params['model_name']
        if base_predictor_record is None:
            raise Exception(f"Error: model '{model_name}' does not exist")

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

        return ModelController.get_model_info(predictor_record)

    def prepare_finetune_statement(self, statement, database_controller):
        project_name, model_name, model_version = resolve_model_identifier(statement.name)
        if project_name is None:
            project_name = default_project
        data_integration_ref, fetch_data_query = self._get_data_integration_ref(statement, database_controller)

        set_active = True
        if statement.using is not None:
            set_active = statement.using.pop('active', True)
            if set_active in ('0', 0, None):
                set_active = False

        label = None
        args = {}
        if statement.using is not None:
            label = statement.using.pop('tag', None)
            args = statement.using

        join_learn_process = args.pop('join_learn_process', False)

        base_predictor_record = get_model_record(
            name=model_name,
            project_name=project_name,
            version=model_version,
            active=True if model_version is None else None
        )

        if data_integration_ref is None:
            data_integration_ref = base_predictor_record.data_integration_ref
        if fetch_data_query is None:
            fetch_data_query = base_predictor_record.fetch_data_query

        return dict(
            model_name=model_name,
            project_name=project_name,
            data_integration_ref=data_integration_ref,
            fetch_data_query=fetch_data_query,
            base_model_version=model_version,
            args=args,
            join_learn_process=join_learn_process,
            label=label,
            set_active=set_active
        )

    @profiler.profile()
    def finetune_model(self, statement, ml_handler):
        params = self.prepare_finetune_statement(statement, ml_handler.database_controller)
        predictor_record = ml_handler.finetune(**params)
        return ModelController.get_model_info(predictor_record)

    def update_model(self, session, project_name: str, model_name: str, problem_definition, version=None):

        model_record = get_model_record(
            name=model_name,
            version=version,
            project_name=project_name,
            except_absent=True
        )
        integration_record = db.Integration.query.get(model_record.integration_id)

        ml_handler_base = session.integration_controller.get_ml_handler(integration_record.name)
        ml_handler_base.update(args=problem_definition, model_id=model_record.id)

        # update model record
        if 'using' in problem_definition:
            learn_args = copy.deepcopy(model_record.learn_args)
            learn_args['using'].update(problem_definition['using'])
            model_record.learn_args = learn_args
            db.session.commit()

    @staticmethod
    def get_model_info(predictor_record):
        from mindsdb.interfaces.database.projects import ProjectController
        projects_controller = ProjectController()
        project = projects_controller.get(id=predictor_record.project_id)

        columns = ['NAME', 'ENGINE', 'PROJECT', 'ACTIVE', 'VERSION', 'STATUS', 'ACCURACY', 'PREDICT', 'UPDATE_STATUS',
                   'MINDSDB_VERSION', 'ERROR', 'SELECT_DATA_QUERY', 'TRAINING_OPTIONS', 'TAG']

        project_name = project.name
        model = project.get_model_by_id(model_id=predictor_record.id)
        table_name = model['name']
        table_meta = model['metadata']
        record = [
            table_name, table_meta['engine'], project_name, table_meta['active'], table_meta['version'], table_meta['status'],
            table_meta['accuracy'], table_meta['predict'], table_meta['update_status'],
            table_meta['mindsdb_version'], table_meta['error'], table_meta['select_data_query'],
            str(table_meta['training_options']), table_meta['label']
        ]

        return pd.DataFrame([record], columns=columns)

    def set_model_active_version(self, project_name, model_name, version):

        model_record = get_model_record(
            name=model_name,
            project_name=project_name,
            version=version,
            active=None
        )

        if model_record is None:
            raise EntityNotExistsError(f'Model {model_name} with version {version} is not found in {project_name}')

        model_record.active = True

        # deactivate current active version
        model_records = db.Predictor.query.filter(
            db.Predictor.name == model_record.name,
            db.Predictor.project_id == model_record.project_id,
            db.Predictor.active == True,    # noqa
            db.Predictor.company_id == ctx.company_id,
            db.Predictor.id != model_record.id
        )
        for p in model_records:
            p.active = False

        db.session.commit()

    def delete_model_version(self, project_name, model_name, version):

        model_record = get_model_record(
            name=model_name,
            project_name=project_name,
            version=version,
            active=None
        )
        if model_record is None:
            raise EntityNotExistsError(f'Model {model_name} with version {version} is not found in {project_name}')

        if model_record.active:
            raise Exception(f"Can't remove active version: {project_name}.{model_name}.{version}")

        is_cloud = self.config.get('cloud', False)
        if is_cloud:
            model_record.deleted_at = dt.datetime.now()
        else:
            db.session.delete(model_record)
        modelStorage = ModelStorage(model_record.id)
        modelStorage.delete()

        db.session.commit()
