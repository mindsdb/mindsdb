import os
import importlib
import traceback
import datetime as dt

from sqlalchemy.orm.attributes import flag_modified

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import Identifier, Select, Star, NativeQuery

from mindsdb.api.executor.sql_query import SQLQuery
import mindsdb.utilities.profiler as profiler
from mindsdb.utilities.functions import mark_process
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log
import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage
from mindsdb.interfaces.model.functions import get_model_records
from mindsdb.integrations.utilities.utils import format_exception_error
from mindsdb.integrations.utilities.sql_utils import make_sql_session
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.integrations.libs.ml_handler_process.handlers_cacher import handlers_cacher

logger = log.getLogger(__name__)


@mark_process(name='learn')
def learn_process(data_integration_ref: dict, problem_definition: dict, fetch_data_query: str,
                  project_name: str, model_id: int, integration_id: int, base_model_id: int,
                  set_active: bool, module_path: str):
    ctx.profiling = {
        'level': 0,
        'enabled': True,
        'pointer': None,
        'tree': None
    }
    profiler.set_meta(query='learn_process', api='http', environment=Config().get('environment'))
    with profiler.Context('learn_process'):
        from mindsdb.interfaces.database.database import DatabaseController

        try:
            predictor_record = db.Predictor.query.with_for_update().get(model_id)
            predictor_record.training_metadata['process_id'] = os.getpid()
            flag_modified(predictor_record, 'training_metadata')
            db.session.commit()

            target = problem_definition.get('target', None)
            training_data_df = None
            if data_integration_ref is not None:
                database_controller = DatabaseController()
                sql_session = make_sql_session()
                if data_integration_ref['type'] == 'integration':
                    integration_name = database_controller.get_integration(data_integration_ref['id'])['name']
                    query = Select(
                        targets=[Star()],
                        from_table=NativeQuery(
                            integration=Identifier(integration_name),
                            query=fetch_data_query
                        )
                    )
                    sqlquery = SQLQuery(query, session=sql_session)
                if data_integration_ref['type'] == 'system':
                    query = Select(
                        targets=[Star()],
                        from_table=NativeQuery(
                            integration=Identifier('log'),
                            query=fetch_data_query
                        )
                    )
                    sqlquery = SQLQuery(query, session=sql_session)
                elif data_integration_ref['type'] == 'view':
                    project = database_controller.get_project(project_name)
                    query_ast = parse_sql(fetch_data_query)
                    view_meta = project.get_view_meta(query_ast)
                    sqlquery = SQLQuery(view_meta['query_ast'], session=sql_session)
                elif data_integration_ref['type'] == 'project':
                    query_ast = parse_sql(fetch_data_query)
                    sqlquery = SQLQuery(query_ast, session=sql_session)

                training_data_df = sqlquery.fetched_data.to_df()

            training_data_columns_count, training_data_rows_count = 0, 0
            if training_data_df is not None:
                training_data_columns_count = len(training_data_df.columns)
                training_data_rows_count = len(training_data_df)

            predictor_record.training_data_columns_count = training_data_columns_count
            predictor_record.training_data_rows_count = training_data_rows_count
            db.session.commit()

            module = importlib.import_module(module_path)

            # check if module is imported successfully and raise exception if not
            if module.import_error is not None:
                raise module.import_error

            handlerStorage = HandlerStorage(integration_id)
            modelStorage = ModelStorage(model_id)
            modelStorage.fileStorage.push()     # FIXME

            kwargs = {}
            if base_model_id is not None:
                kwargs['base_model_storage'] = ModelStorage(base_model_id)
                kwargs['base_model_storage'].fileStorage.pull()
            ml_handler = module.Handler(
                engine_storage=handlerStorage,
                model_storage=modelStorage,
                **kwargs
            )
            handlers_cacher[predictor_record.id] = ml_handler

            if not ml_handler.generative and target is not None:
                if training_data_df is not None and target not in training_data_df.columns:
                    # is the case different? convert column case in input dataframe
                    col_names = {c.lower(): c for c in training_data_df.columns}
                    target_found = col_names.get(target.lower())
                    if target_found:
                        training_data_df.rename(columns={target_found: target}, inplace=True)
                    else:
                        raise Exception(
                            f'Prediction target "{target}" not found in training dataframe: {list(training_data_df.columns)}')

            # create new model
            if base_model_id is None:
                with profiler.Context('create'):
                    ml_handler.create(target, df=training_data_df, args=problem_definition)

            # fine-tune (partially train) existing model
            else:
                # load model from previous version, use it as starting point
                with profiler.Context('finetune'):
                    problem_definition['base_model_id'] = base_model_id
                    ml_handler.finetune(df=training_data_df, args=problem_definition)

            predictor_record.status = PREDICTOR_STATUS.COMPLETE
            predictor_record.active = set_active
            db.session.commit()
            # if retrain and set_active after success creation
            if set_active is True:
                models = get_model_records(
                    name=predictor_record.name,
                    project_id=predictor_record.project_id,
                    active=None
                )
                for model in models:
                    model.active = False
                models = [x for x in models if x.status == PREDICTOR_STATUS.COMPLETE]
                models.sort(key=lambda x: x.created_at)
                models[-1].active = True
        except Exception as e:
            logger.error(traceback.format_exc())
            error_message = format_exception_error(e)

            predictor_record = db.Predictor.query.with_for_update().get(model_id)
            predictor_record.data = {"error": error_message}
            predictor_record.status = PREDICTOR_STATUS.ERROR
            db.session.commit()

        predictor_record.training_stop_at = dt.datetime.now()
        db.session.commit()
