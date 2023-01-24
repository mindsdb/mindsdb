"""
This module defines the wrapper for ML engines which abstracts away a lot of complexity.

In particular, three big components are included:

    - `BaseMLEngineExec` class: this class wraps any object that inherits from `BaseMLEngine` and exposes some endpoints
      normally associated with a DB handler (e.g. `native_query`, `get_tables`), as well as other ML-specific behaviors,
      like `learn()` or `predict()`. Note that while these still have to be implemented at the engine level, the burden
      on that class is lesser given that it only needs to return a pandas DataFrame. It's this class that will take said
      output and format it into the HandlerResponse instance that MindsDB core expects.

    - `learn_process` method: handles async dispatch of the `learn` method in an engine, as well as registering all
      models inside of the internal MindsDB registry.

    - `predict_process` method: handles async dispatch of the `predict` method in an engine.

"""

import datetime as dt
import traceback
import importlib
from typing import Optional

import pandas as pd

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Identifier, Select, Show, Star, NativeQuery

from mindsdb.integrations.utilities.utils import make_sql_session, get_where_data
from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb import __version__ as mindsdb_version
from mindsdb.utilities.hooks import after_predict as after_predict_hook
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.model.functions import (
    get_model_record
)
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import SQLQuery
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.integrations.utilities.processes import HandlerProcess
from mindsdb.utilities.functions import mark_process
from mindsdb.integrations.utilities.utils import format_exception_error
from mindsdb.interfaces.database.database import DatabaseController
from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage
from mindsdb.utilities.context import context as ctx
from mindsdb.interfaces.model.functions import get_model_records

from .ml_handler_proc import MLHandlerWrapper, MLHandlerPersistWrapper

import torch.multiprocessing as mp
mp.get_context('spawn')


@mark_process(name='learn')
def learn_process(class_path, context_dump, integration_id,
                  predictor_id, data_integration_ref, fetch_data_query,
                  project_name, problem_definition, set_active,
                  base_predictor_id=None):
    ctx.load(context_dump)
    db.init()

    predictor_record = db.Predictor.query.with_for_update().get(predictor_id)

    try:
        target = problem_definition['target']
        training_data_df = None

        database_controller = DatabaseController()

        sql_session = make_sql_session()
        if data_integration_ref is not None:
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

            if target not in training_data_df.columns:
                raise Exception(
                    f'Prediction target "{target}" not found in training dataframe: {list(training_data_df.columns)}')

        predictor_record.training_data_columns_count = training_data_columns_count
        predictor_record.training_data_rows_count = training_data_rows_count
        db.session.commit()

        module_name, class_name = class_path
        module = importlib.import_module(module_name)
        HandlerClass = getattr(module, class_name)

        handlerStorage = HandlerStorage(integration_id)
        modelStorage = ModelStorage(predictor_id)

        ml_handler = HandlerClass(
            engine_storage=handlerStorage,
            model_storage=modelStorage,
        )

        # create new model
        if base_predictor_id is None:
            ml_handler.create(target, df=training_data_df, args=problem_definition)

        # adjust (partially train) existing model
        else:
            # load model from previous version, use it as starting point
            problem_definition['base_model_id'] = base_predictor_id
            ml_handler.update(df=training_data_df, args=problem_definition)

        predictor_record.status = PREDICTOR_STATUS.COMPLETE
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
        print(traceback.format_exc())
        error_message = format_exception_error(e)

        predictor_record.data = {"error": error_message}
        predictor_record.status = PREDICTOR_STATUS.ERROR
        db.session.commit()

    predictor_record.training_stop_at = dt.datetime.now()
    db.session.commit()


class BaseMLEngineExec:

    def __init__(self, name, **kwargs):
        """
        ML handler interface converter
        """  # noqa
        # TODO move this class to model controller

        self.name = name
        self.config = Config()
        self.handler_controller = kwargs.get('handler_controller')
        self.company_id = kwargs.get('company_id')
        self.fs_store = kwargs.get('file_storage')
        self.storage_factory = kwargs.get('storage_factory')
        self.integration_id = kwargs.get('integration_id')
        self.execution_method = kwargs.get('execution_method')

        self.model_controller = ModelController()

        self.database_controller = DatabaseController()

        self.parser = parse_sql
        self.dialect = 'mindsdb'

        self.is_connected = True

        self.handler_class = kwargs['handler_class']

    def get_ml_handler(self, predictor_id=None):
        # returns instance or wrapper over it

        integration_id = self.integration_id

        class_path = [self.handler_class.__module__, self.handler_class.__name__]

        if self.execution_method == 'subprocess':
            handler = MLHandlerWrapper()

            handler.init_handler(class_path, integration_id, predictor_id, ctx.dump())
            return handler

        elif self.execution_method == 'subprocess_keep':
            handler = MLHandlerPersistWrapper()

            handler.init_handler(class_path, integration_id, predictor_id, ctx.dump())
            return handler

        elif self.execution_method == 'remote':
            raise NotImplementedError()

        else:
            handlerStorage = HandlerStorage(integration_id)
            modelStorage = ModelStorage(predictor_id)

            ml_handler = self.handler_class(
                engine_storage=handlerStorage,
                model_storage=modelStorage,
            )
            return ml_handler

    def get_tables(self) -> Response:
        """ Returns all models currently registered that belong to the ML engine."""
        all_models = self.model_controller.get_models(integration_id=self.integration_id)
        all_models_names = [[x['name']] for x in all_models]
        response = Response(
            RESPONSE_TYPE.TABLE,
            pd.DataFrame(
                all_models_names,
                columns=['table_name']
            )
        )
        return response

    def get_columns(self, table_name: str) -> Response:
        """ Retrieves standard info about a model, e.g. data types. """  # noqa
        predictor_record = get_model_record(name=table_name, ml_handler_name=self.name)
        if predictor_record is None:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Error: model '{table_name}' does not exist!"
            )

        data = []
        if predictor_record.dtype_dict is not None:
            for key, value in predictor_record.dtype_dict.items():
                data.append((key, value))
        result = Response(
            RESPONSE_TYPE.TABLE,
            pd.DataFrame(
                data,
                columns=['COLUMN_NAME', 'DATA_TYPE']
            )
        )
        return result

    def native_query(self, query: str) -> Response:
        """ Intakes a raw SQL query and returns the answer given by the ML engine. """
        query_ast = self.parser(query, dialect=self.dialect)
        return self.query(query_ast)

    def query_(self, query: ASTNode) -> Response:
        raise Exception('Should not be used')

        """ Intakes a pre-parsed SQL query (via `mindsdb_sql`) and returns the answer given by the ML engine. """
        statement = query

        if type(statement) == Show:
            if statement.category.lower() == 'tables':
                return self.get_tables()
            else:
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Cant determine how to show '{statement.category}'"
                )
            return response
        elif type(statement) == Select:
            model_name = statement.from_table.parts[-1]
            where_data = get_where_data(statement.where)
            predictions = self.predict(model_name, where_data)
            return Response(
                RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame(predictions)
            )
        else:
            raise Exception(f"Query type {type(statement)} not supported")

    def learn(
        self, model_name, project_name,
        data_integration_ref=None,
        fetch_data_query=None,
        problem_definition=None,
        join_learn_process=False,
        label=None,
        version=1,
        is_retrain=False,
        set_active=True,
    ):
        # TODO move to model_controller
        """ Trains a model given some data-gathering SQL statement. """

        target = problem_definition['target']

        project = self.database_controller.get_project(name=project_name)

        # handler-side validation
        if hasattr(self.handler_class, 'create_validation'):
            self.handler_class.create_validation(target, args=problem_definition)

        predictor_record = db.Predictor(
            company_id=ctx.company_id,
            name=model_name,
            integration_id=self.integration_id,
            data_integration_ref=data_integration_ref,
            fetch_data_query=fetch_data_query,
            mindsdb_version=mindsdb_version,
            to_predict=target,
            learn_args=problem_definition,
            data={'name': model_name},
            project_id=project.id,
            training_data_columns_count=None,
            training_data_rows_count=None,
            training_start_at=dt.datetime.now(),
            status=PREDICTOR_STATUS.GENERATING,
            label=label,
            version=version,
            active=(not is_retrain),  # if create then active
        )

        db.session.add(predictor_record)
        db.session.commit()

        class_path = [self.handler_class.__module__, self.handler_class.__name__]

        p = HandlerProcess(
            learn_process,
            class_path,
            ctx.dump(),
            self.integration_id,
            predictor_record.id,
            data_integration_ref,
            fetch_data_query,
            project_name,
            problem_definition,
            set_active
        )
        p.start()
        if join_learn_process is True:
            p.join()

    def predict(self, model_name: str, data: list, pred_format: str = 'dict',
                project_name: str = None, version=None, params: dict = None):
        """ Generates predictions with some model and input data. """
        if isinstance(data, dict):
            data = [data]
        df = pd.DataFrame(data)
        predictor_record = get_model_record(
            name=model_name, ml_handler_name=self.name, project_name=project_name,
            version=version
        )
        if predictor_record is None:
            if version is not None:
                model_name = f'{model_name}.{version}'
            raise Exception(f"Error: model '{model_name}' does not exists!")

        ml_handler = self.get_ml_handler(predictor_record.id)

        args = {
            'pred_format': pred_format,
            'predict_params': {} if params is None else params
        }
        # FIXME
        if self.handler_class.__name__ == 'LightwoodHandler':
            args['code'] = predictor_record.code
            args['target'] = predictor_record.to_predict[0]
            args['dtype_dict'] = predictor_record.dtype_dict
            args['learn_args'] = predictor_record.learn_args

        predictions = ml_handler.predict(df, args)

        ml_handler.close()

        # mdb indexes
        if '__mindsdb_row_id' not in predictions.columns and '__mindsdb_row_id' in df.columns:
            predictions['__mindsdb_row_id'] = df['__mindsdb_row_id']

        after_predict_hook(
            company_id=self.company_id,
            predictor_id=predictor_record.id,
            rows_in_count=df.shape[0],
            columns_in_count=df.shape[1],
            rows_out_count=len(predictions)
        )
        return predictions

    def update(
            self, model_name, project_name, version,
            data_integration_ref=None,
            fetch_data_query=None,
            join_learn_process=False,
            label=None,
            set_active=True,
            args: Optional[dict] = None
    ):
        # generate new record from latest version as starting point
        project = self.database_controller.get_project(name=project_name)
        predictor_records = get_model_records(
            active=None,
            name=model_name,
        )
        predictor_records = [
            x for x in predictor_records
            if x.training_stop_at is not None
        ]
        predictor_records.sort(key=lambda x: x.training_stop_at, reverse=True)

        base_predictor_record = predictor_records[0]

        predictor_record = db.Predictor(
            company_id=ctx.company_id,
            name=model_name,
            integration_id=self.integration_id,
            data_integration_ref=data_integration_ref,
            fetch_data_query=fetch_data_query,
            mindsdb_version=mindsdb_version,
            to_predict=base_predictor_record.to_predict,
            learn_args=base_predictor_record.learn_args,
            data={'name': model_name},
            project_id=project.id,
            training_data_columns_count=None,
            training_data_rows_count=None,
            training_start_at=dt.datetime.now(),
            status=PREDICTOR_STATUS.GENERATING,
            label=label,
            version=version,
            active=False
        )

        db.session.add(predictor_record)
        db.session.commit()

        class_path = [self.handler_class.__module__, self.handler_class.__name__]

        p = HandlerProcess(
            learn_process,
            class_path,
            ctx.dump(),
            self.integration_id,
            predictor_record.id,
            data_integration_ref,
            fetch_data_query,
            project_name,
            predictor_record.learn_args,
            set_active,
            base_predictor_record.id,
        )
        p.start()
        if join_learn_process is True:
            p.join()
