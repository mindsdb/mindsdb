import datetime as dt
from dateutil.parser import parse as parse_datetime
import json

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

from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.interfaces.database.views import ViewController
from mindsdb.integrations.libs.base_handler import PredictiveHandler
from mindsdb.utilities.config import Config
import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb import __version__ as mindsdb_version
from mindsdb.utilities.functions import cast_row_types
from mindsdb.utilities.hooks import after_predict as after_predict_hook
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.model.functions import (
    get_model_record,
    get_model_records
)
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import SQLQuery

from mindsdb.integrations.libs.const import PREDICTOR_STATUS

from .storage import ModelStorage, HandlerStorage
from .byom_handler import BYOMHandler


def get_where_data(where):
    result = {}
    if type(where) != BinaryOperation:
        raise Exception("Wrong 'where' statement")
    if where.op == '=':
        if type(where.args[0]) != Identifier or type(where.args[1]) != Constant:
            raise Exception("Wrong 'where' statement")
        result[where.args[0].parts[-1]] = where.args[1].value
    elif where.op == 'and':
        result.update(get_where_data(where.args[0]))
        result.update(get_where_data(where.args[1]))
    else:
        raise Exception("Wrong 'where' statement")
    return result


class NumpyJSONEncoder(json.JSONEncoder):
    """
    Use this encoder to avoid
    "TypeError: Object of type float32 is not JSON serializable"

    Example:
    x = np.float32(5)
    json.dumps(x, cls=NumpyJSONEncoder)
    """
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.float, np.float32, np.float64)):
            return float(obj)
        else:
            return super().default(obj)


class BYOMHandler_EXECUTOR(PredictiveHandler):

    name = 'byom'

    def __init__(self, name, **kwargs):
        """
        Handler for BYOM
        """  # noqa
        super().__init__(name)

        self.name = name
        self.config = Config()
        self.handler_controller = kwargs.get('handler_controller')
        self.company_id = kwargs.get('company_id')
        self.fs_store = kwargs.get('fs_store')
        self.company_id = kwargs.get('company_id')
        self.integration_id = kwargs.get('integration_id')

        self.handlerStorage = HandlerStorage(
            fs_store=self.fs_store,
            company_id=self.company_id,
            integration_id=self.integration_id,
        )

        self.model_controller = WithKWArgsWrapper(
            ModelController(),
            company_id=self.company_id
        )

        self.parser = parse_sql
        self.dialect = 'mindsdb'

        self.is_connected = True

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

    def make_sql_session(self, company_id):

        server_obj = type('', (), {})()
        server_obj.original_integration_controller = IntegrationController()
        server_obj.original_model_controller = ModelController()
        server_obj.original_view_controller = ViewController()

        sql_session = SessionController(
            server=server_obj,
            company_id=company_id
        )
        sql_session.database = 'mindsdb'
        return sql_session

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

        # get data for learn
        integration_name = statement.integration_name.parts[0]

        # get data from integration
        query = Select(
            targets=[Star()],
            from_table=NativeQuery(
                integration=Identifier(integration_name),
                query=statement.query_str
            )
        )
        sql_session = self.make_sql_session(self.company_id)

        # execute as query
        sqlquery = SQLQuery(query, session=sql_session)
        result = sqlquery.fetch(view='dataframe')

        training_data_df = result['result']

        integration_meta = self.handler_controller.get(name=integration_name)

        problem_definition = {
            'target': target
        }

        predictor_record = db.Predictor(
            company_id=self.company_id,
            name=model_name,
            integration_id=self.integration_id,
            data_integration_id=integration_meta['id'],
            fetch_data_query=statement.query_str,
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

        db.session.refresh(predictor_record)

        model_storage = ModelStorage(
            fs_store=self.fs_store,
            company_id=self.company_id,
            integration_id=self.integration_id,
            predictor_id=predictor_record.id
        )
        ml_handler = BYOMHandler(
            handler_storage=self.handlerStorage,
            model_storage=model_storage,
        )

        # TODO run it in subprocess

        ml_handler.learn(training_data_df, target)

        return Response(RESPONSE_TYPE.OK)

    def retrain(self, statement):
        # TODO
        #  mark current predictor as inactive
        #  create new predictor and run learn
        raise NotImplementedError()

    def predict(self, model_name: str, data: list, pred_format: str = 'dict') -> pd.DataFrame:
        if isinstance(data, dict):
            data = [data]
        df = pd.DataFrame(data)
        predictor_record = get_model_record(company_id=self.company_id, name=model_name, ml_handler_name=self.name)
        if predictor_record is None:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Error: model '{model_name}' does not exists!"
            )

        model_storage = ModelStorage(
            fs_store=self.fs_store,
            company_id=self.company_id,
            integration_id=self.integration_id,
            predictor_id=predictor_record.id
        )
        ml_handler = BYOMHandler(
            handler_storage=self.handlerStorage,
            model_storage=model_storage,
        )

        predictions = ml_handler.predict(df)
        predictions = predictions.to_dict(orient='records')

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
