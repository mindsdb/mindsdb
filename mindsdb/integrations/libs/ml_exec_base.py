from collections import OrderedDict

import datetime as dt
from dateutil.parser import parse as parse_datetime
import json
import traceback
import importlib

import numpy as np
import pandas as pd

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import BinaryOperation, Identifier, Constant, Select, Show, Star, NativeQuery
from mindsdb_sql.parser.dialects.mindsdb import (
    RetrainPredictor,
    CreatePredictor,
    DropPredictor
)


from mindsdb.integrations.utilities.utils import make_sql_session, get_where_data

from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb import __version__ as mindsdb_version
from mindsdb.utilities.hooks import after_predict as after_predict_hook
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.model.functions import (
    get_model_record,
    get_model_records
)
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import SQLQuery

from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.integrations.utilities.processes import HandlerProcess
from mindsdb.utilities.functions import mark_process
from mindsdb.integrations.utilities.utils import format_exception_error

from mindsdb.interfaces.storage.fs import ModelStorage, HandlerStorage

import torch.multiprocessing as mp
ctx = mp.get_context('spawn')

@mark_process(name='learn')
def learn_process(class_path, company_id, integration_id, predictor_id, training_data_df, target, problem_definition):

    predictor_record = db.Predictor.query.with_for_update().get(predictor_id)

    predictor_record.training_start_at = dt.datetime.now()
    db.session.commit()

    try:
        module_name, class_name = class_path
        module = importlib.import_module(module_name)
        klass = getattr(module, class_name)

        handlerStorage = HandlerStorage(company_id, integration_id)
        modelStorage = ModelStorage(company_id, predictor_id)

        ml_handler = klass(
            engine_storage=handlerStorage,
            model_storage=modelStorage,
        )
        ml_handler.create(target, df=training_data_df, args=problem_definition)

    except Exception as e:
        print(traceback.format_exc())
        error_message = format_exception_error(e)

        predictor_record.data = {"error": error_message}
        predictor_record.status = PREDICTOR_STATUS.ERROR
        db.session.commit()

    predictor_record.training_stop_at = dt.datetime.now()
    predictor_record.status = PREDICTOR_STATUS.COMPLETE
    db.session.commit()


@mark_process(name='predict')
def predict_process(class_path, company_id, integration_id, predictor_id, df, res_queue=None):

    module_name, class_name = class_path
    module = importlib.import_module(module_name)
    klass = getattr(module, class_name)

    handlerStorage = HandlerStorage(company_id, integration_id)
    modelStorage = ModelStorage(company_id, predictor_id)

    ml_handler = klass(
        engine_storage=handlerStorage,
        model_storage=modelStorage,
    )

    predictions = ml_handler.predict(df)

    # mdb indexes
    if '__mindsdb_row_id' not in predictions.columns:
        predictions['__mindsdb_row_id'] = df['__mindsdb_row_id']

    predictions = predictions.to_dict(orient='records')

    if res_queue is not None:
        # subprocess mode
        res_queue.put(predictions)
    else:
        return predictions

class BaseMLEngineExec:

    def __init__(self, name, **kwargs):
        """
        ML handler interface converter
        """  # noqa

        self.name = name
        self.config = Config()
        self.handler_controller = kwargs.get('handler_controller')
        self.company_id = kwargs.get('company_id')
        self.fs_store = kwargs.get('file_storage')
        self.storage_factory = kwargs.get('storage_factory')
        self.integration_id = kwargs.get('integration_id')

        self.model_controller = WithKWArgsWrapper(
            ModelController(),
            company_id=self.company_id
        )

        self.parser = parse_sql
        self.dialect = 'mindsdb'

        self.is_connected = True

        self.handler_class = kwargs['handler_class']

    def get_tables(self) -> Response:
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
        """ For getting standard info about a table. e.g. data types """  # noqa
        predictor_record = get_model_record(company_id=self.company_id, name=table_name, ml_handler_name=self.name)
        if predictor_record is None:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Error: model '{table_name}' does not exists!"
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
        query_ast = self.parser(query, dialect=self.dialect)
        return self.query(query_ast)

    def query(self, query: ASTNode) -> Response:
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
        if type(statement) == CreatePredictor:
            return self.learn(statement)
        elif type(statement) == RetrainPredictor:
            return self.retrain(statement)
        elif type(statement) == DropPredictor:
            return self.drop(statement)
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

    def learn(self, statement):
        model_name = statement.name.parts[-1]

        data = self.get_tables().data_frame.to_dict(orient='records')
        tables_names = [x['table_name'] for x in data]

        if model_name in tables_names:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message="Error: this model already exists!"
            )

        target = statement.targets[0].parts[-1]
        training_data_df = pd.DataFrame()
        fetch_data_query = None
        data_integration_id = None
        # get data for learn
        if statement.integration_name is not None:
            fetch_data_query = statement.query_str
            integration_name = statement.integration_name.parts[0]

            # get data from integration
            query = Select(
                targets=[Star()],
                from_table=NativeQuery(
                    integration=Identifier(integration_name),
                    query=statement.query_str
                )
            )
            sql_session = make_sql_session(self.company_id)

            # execute as query
            sqlquery = SQLQuery(query, session=sql_session)
            result = sqlquery.fetch(view='dataframe')

            training_data_df = result['result']

            data_integration_id = self.handler_controller.get(name=integration_name)['id']

        problem_definition = statement.using
        if problem_definition is None:
            problem_definition = {}

        problem_definition['target'] = target

        predictor_record = db.Predictor(
            company_id=self.company_id,
            name=model_name,
            integration_id=self.integration_id,
            data_integration_id=data_integration_id,
            fetch_data_query=fetch_data_query,
            mindsdb_version=mindsdb_version,
            to_predict=target,
            learn_args=problem_definition,
            data={'name': model_name},
            training_data_columns_count=len(training_data_df.columns),
            training_data_rows_count=len(training_data_df),
            training_start_at=dt.datetime.now(),
            status=PREDICTOR_STATUS.GENERATING
        )

        db.session.add(predictor_record)
        db.session.commit()

        class_path = [self.handler_class.__module__, self.handler_class.__name__]

        p = HandlerProcess(
            learn_process,
            class_path,
            self.company_id,
            self.integration_id,
            predictor_record.id,
            training_data_df,
            target,
            problem_definition,
        )
        p.start()

        return Response(RESPONSE_TYPE.OK)

    def retrain(self, statement):
        # TODO
        #  mark current predictor as inactive
        #  create new predictor and run learn
        raise NotImplementedError()

    def predict(self, model_name: str, data: list, pred_format: str = 'dict'):
        if isinstance(data, dict):
            data = [data]
        df = pd.DataFrame(data)
        predictor_record = get_model_record(company_id=self.company_id, name=model_name, ml_handler_name=self.name)
        if predictor_record is None:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Error: model '{model_name}' does not exists!"
            )

        class_path = [self.handler_class.__module__, self.handler_class.__name__]

        is_subprocess = False
        if is_subprocess:

            res_queue = ctx.SimpleQueue()
            p = HandlerProcess(
                predict_process,
                class_path,
                self.company_id,
                self.integration_id,
                predictor_record.id,
                df,
                res_queue,
            )
            p.start()
            p.join()
            predictions = res_queue.get()

        else:
            predictions = predict_process(
                class_path,
                self.company_id,
                self.integration_id,
                predictor_record.id,
                df
            )

        after_predict_hook(
            company_id=self.company_id,
            predictor_id=predictor_record.id,
            rows_in_count=df.shape[0],
            columns_in_count=df.shape[1],
            rows_out_count=len(predictions)
        )
        return predictions

    def drop(self, statement):
        model_name = statement.name.parts[-1]

        predictors_records = get_model_records(
            company_id=self.company_id,
            name=model_name,
            ml_handler_name=self.name
        )
        if len(predictors_records) == 0:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Model '{model_name}' does not exist"
            )

        is_cloud = self.config.get('cloud', False)
        if is_cloud:
            for predictor_record in predictors_records:
                model_data = self.model_controller.get_model_data(predictor_record=predictor_record)
                if (
                    is_cloud is True
                    and model_data.get('status') in ['generating', 'training']
                    and isinstance(model_data.get('created_at'), str) is True
                    and (dt.datetime.now() - parse_datetime(model_data.get('created_at'))) < dt.timedelta(hours=1)
                ):
                    raise Exception('You are unable to delete models currently in progress, please wait before trying again')

        for predictor_record in predictors_records:
            if is_cloud:
                predictor_record.deleted_at = dt.datetime.now()
            else:
                db.session.delete(predictor_record)
            self.fs_store.delete(f'predictor_{self.company_id}_{predictor_record.id}')
        db.session.commit()
        return Response(RESPONSE_TYPE.OK)





