import importlib
import traceback
import datetime as dt

from mindsdb_sql.parser.ast import Identifier, Select, Star, NativeQuery

from mindsdb.utilities.functions import mark_process
from mindsdb.utilities.context import context as ctx
import mindsdb.interfaces.storage.db as db
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import SQLQuery
from mindsdb.integrations.utilities.sql_utils import make_sql_session
from mindsdb_sql import parse_sql
from mindsdb.integrations.handlers_client.ml_client_factory import MLClientFactory
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage
from mindsdb.interfaces.model.functions import get_model_records
from mindsdb.integrations.utilities.utils import format_exception_error
import mindsdb.utilities.profiler as profiler
from mindsdb.utilities.config import Config


@mark_process(name='learn')
def learn_process(class_path, engine, context_dump, integration_id,
                  predictor_id, problem_definition, set_active,
                  base_predictor_id=None, training_data_df=None,
                  data_integration_ref=None, fetch_data_query=None, project_name=None):
    ctx.load(context_dump)
    ctx.profiling = {
        'level': 0,
        'enabled': True,
        'pointer': None,
        'tree': None
    }
    profiler.set_meta(query='learn_process', api='http', environment=Config().get('environment'))
    with profiler.Context('learn_process'):
        from mindsdb.interfaces.database.database import DatabaseController
        db.init()

        try:
            target = problem_definition['target']

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
                elif data_integration_ref['type'] == 'view':
                    project = database_controller.get_project(project_name)
                    query_ast = parse_sql(fetch_data_query, dialect='mindsdb')
                    view_query_ast = project.query_view(query_ast)
                    sqlquery = SQLQuery(view_query_ast, session=sql_session)

                result = sqlquery.fetch(view='dataframe')
                training_data_df = result['result']

            training_data_columns_count, training_data_rows_count = 0, 0
            if training_data_df is not None:
                training_data_columns_count = len(training_data_df.columns)
                training_data_rows_count = len(training_data_df)

            predictor_record = db.Predictor.query.with_for_update().get(predictor_id)
            predictor_record.training_data_columns_count = training_data_columns_count
            predictor_record.training_data_rows_count = training_data_rows_count
            db.session.commit()

            module_name, class_name = class_path
            module = importlib.import_module(module_name)
            HandlerClass = getattr(module, class_name)
            HandlerClass = MLClientFactory(handler_class=HandlerClass, engine=engine)

            handlerStorage = HandlerStorage(integration_id)
            modelStorage = ModelStorage(predictor_id)

            kwargs = {}
            if base_predictor_id is not None:
                kwargs['base_model_storage'] = ModelStorage(base_predictor_id)
                kwargs['base_model_storage'].fileStorage.pull()

            ml_handler = HandlerClass(
                engine_storage=handlerStorage,
                model_storage=modelStorage,
                **kwargs
            )

            if not ml_handler.generative:
                if training_data_df is not None and target not in training_data_df.columns:
                    raise Exception(
                        f'Prediction target "{target}" not found in training dataframe: {list(training_data_df.columns)}')

            # create new model
            if base_predictor_id is None:
                ml_handler.create(target, df=training_data_df, args=problem_definition)

            # fine-tune (partially train) existing model
            else:
                # load model from previous version, use it as starting point
                problem_definition['base_model_id'] = base_predictor_id
                ml_handler.finetune(df=training_data_df, args=problem_definition)

            predictor_record.status = PREDICTOR_STATUS.COMPLETE
            predictor_record.active = set_active
            db.session.commit()
            # if retrain and set_active after success creation
            with profiler.Context('learn_process update active'):
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
            print(traceback.format_exc())
            error_message = format_exception_error(e)

            predictor_record = db.Predictor.query.with_for_update().get(predictor_id)
            predictor_record.data = {"error": error_message}
            predictor_record.status = PREDICTOR_STATUS.ERROR
            db.session.commit()

        predictor_record.training_stop_at = dt.datetime.now()
        db.session.commit()
