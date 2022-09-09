from typing import Any, Dict

import ray
import dask
import dill
import sqlalchemy
import pandas as pd
import datetime
from dateutil.parser import parse as parse_datetime
from ludwig import __version__ as ludwig_version
from ludwig.automl import auto_train

from mindsdb_sql import parse_sql
from mindsdb import __version__ as mindsdb_version
from mindsdb.utilities.log import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import mark_process
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.utilities.hooks import after_predict as after_predict_hook
import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import BinaryOperation, Identifier, Constant, Select, Show, Star, NativeQuery
from mindsdb.integrations.utilities.utils import make_sql_session, get_where_data
from mindsdb.integrations.libs.storage_handler import SqliteStorageHandler
from mindsdb.integrations.libs.base_handler import PredictiveHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse,
    HandlerResponse,
    RESPONSE_TYPE
)
from mindsdb.interfaces.model.functions import (
    get_model_record,
    get_model_records
)
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import SQLQuery
from mindsdb_sql.parser.dialects.mindsdb import (
    CreatePredictor,
    RetrainPredictor,
    DropPredictor,
)


class LudwigHandler(PredictiveHandler):

    name = 'ludwig'
    predictor_cache: Dict[str, Dict[str, Any]]

    def __init__(self, name, **kwargs):
        """
        Handler to create and use Ludwig AutoML models from MindsDB.
        """  # noqa
        super().__init__(name)
        self.config = Config()
        self.predictor_cache = {}
        storage_config = kwargs.get('storage_config', {'name': "ludwig_handler_storage"})
        self.storage = SqliteStorageHandler(context={}, config=storage_config)
        for key in ('models', 'metadata'):
            try:
                self.storage.get(key)
            except KeyError:
                self.storage.set(key, {})

        self.parser = parse_sql
        self.dialect = 'mindsdb'
        self.handler_dialect = 'mysql'

        self.handler_controller = kwargs.get('handler_controller')
        self.fs_store = kwargs.get('fs_store')
        self.company_id = kwargs.get('company_id')
        self.model_controller = WithKWArgsWrapper(
            ModelController(),
            company_id=self.company_id
        )

        self.dtypes_to_sql = {
            "Number": sqlalchemy.Integer,
            "Binary": sqlalchemy.Text,
            "Category": sqlalchemy.Text,
            "Bag": sqlalchemy.Text,
            "Set": sqlalchemy.Text,
            "Date": sqlalchemy.DateTime,
            "Text": sqlalchemy.Text,
            "H3": sqlalchemy.Text,
            "Sequence": sqlalchemy.Text,
            "Vector": sqlalchemy.Text,
        }

    def check_connection(self, **kwargs) -> HandlerStatusResponse:
        """ Setup storage and check whether Ludwig is available. """  # noqa
        result = HandlerStatusResponse(False)
        try:
            import ludwig as lw
            from ludwig.api import LudwigModel
            from ludwig.automl import auto_train
            result.success = True
            self.is_connected = True
        except ImportError as e:
            log.error(f'Error importing Ludwig, {e}!')
            result.error_message = str(e)
        return result

    def get_tables(self) -> HandlerResponse:
        """ Returns name list of trained models.  """  # noqa

        models = self.storage.get('models', [])
        if models:
            df = pd.DataFrame(
                list(models.keys()) if models else [],
                columns=['table_name']
            )
        else:
            df = pd.DataFrame(columns=['table_name'])

        return HandlerResponse(RESPONSE_TYPE.TABLE, df)

    def get_columns(self, table_name: str) -> HandlerResponse:
        """ For any given model, return the input data types. """  # noqa
        try:
            model = self.storage.get('models', {}).get(table_name, {}).get('model', None)
            if not model:
                df = pd.DataFrame(columns=['COLUMN_NAME', 'DATA_TYPE'])
            else:
                model = dill.loads(model)
                cfg = model.config
                df = pd.DataFrame(
                    [v for v in cfg.values()],
                    columns=[k for k in cfg.keys()]
                )
            r = HandlerResponse(RESPONSE_TYPE.TABLE, df)
        except Exception as e:
            log.error(f"Could not get columns for model {table_name}, error: {e}")
            r = HandlerResponse(RESPONSE_TYPE.ERROR)
        return r

    def native_query(self, query: Any) -> HandlerResponse:
        statement = self.parser(query, dialect=self.dialect)
        return self.query(statement)

    def query(self, query: ASTNode) -> HandlerResponse:
        """old query body
        values = recur_get_conditionals(query.where.args, {})
        model_name, _, _ = self._get_model_name(query)
        model = self._get_model(model_name)
        df = pd.DataFrame.from_dict(values)
        df = self._call_model(df, model)
        r = HandlerResponse(
            RESPONSE_TYPE.TABLE,
            df
        )
        return r
        """
        if type(query) == CreatePredictor:
            with RayConnection():
                self._learn(query)

        elif type(query) == RetrainPredictor:
            msg = 'Warning: retraining Ludwig models is not yet supported!'  # TODO: restore this
            return HandlerResponse(RESPONSE_TYPE.ERROR, error_message=msg)

        elif type(query) == DropPredictor:
            to_drop = query.name.parts[-1]
            models = self.storage.get('models')
            if models:
                # @TODO: this could be a common helper method
                predictors_records = get_model_records(company_id=self.company_id, name=to_drop, active=None)
                if len(predictors_records) == 0:
                    return HandlerResponse(
                        RESPONSE_TYPE.ERROR,
                        error_message=f"Model '{to_drop}' does not exist"
                    )

                is_cloud = self.config.get('cloud', False)
                if is_cloud:
                    for predictor_record in predictors_records:
                        model_data = self.model_controller.get_model_data(predictor_record=predictor_record)
                        if (
                            is_cloud is True
                            and model_data.get('status') in ['generating', 'training']
                            and isinstance(model_data.get('created_at'), str) is True
                            and (datetime.datetime.now() - parse_datetime(model_data.get('created_at'))) < datetime.timedelta(hours=1)
                        ):
                            raise Exception('You are unable to delete models currently in progress, please wait before trying again')

                for predictor_record in predictors_records:
                    if is_cloud:
                        predictor_record.deleted_at = datetime.datetime.now()
                        predictor_record.status = PREDICTOR_STATUS.DELETED
                    else:
                        db.session.delete(predictor_record)
                    self.fs_store.delete(f'predictor_{self.company_id}_{predictor_record.id}')
                db.session.commit()

                # end common method

                del models[to_drop]
                self.storage.set('models', models)

                return HandlerResponse(RESPONSE_TYPE.OK)
            else:
                raise Exception(f"Can't drop non-existent model {to_drop}")

        elif type(query) == Select:
            model_name = query.from_table.parts[-1]

            if not self._get_model(model_name):
                return HandlerResponse(
                    RESPONSE_TYPE.ERROR,
                    error_message=f"Error: model '{model_name}' does not exist!"
                )

            where_data = get_where_data(query.where)
            predictions = self.predict(model_name, where_data)
            return HandlerResponse(
                RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame(predictions)
            )
        else:
            raise Exception(f"Query type {type(query)} not supported")

        return HandlerResponse(RESPONSE_TYPE.OK)

    # def join(self, stmt, data_handler, into: Optional[str]) -> HandlerResponse:
    #     """
    #     Batch prediction using the output of a query passed to a data handler as input for the model.
    #     """  # noqa
    #
    #     model_name, model_alias, model_side = self._get_model_name(stmt)
    #     data_side = 'right' if model_side == 'left' else 'left'
    #     model = self._get_model(model_name)
    #     model_input = get_join_input(stmt, model, [model_name, model_alias], data_handler, data_side)
    #
    #     # get model output and rename columns
    #     predictions = self._call_model(model_input, model)
    #     model_input.columns = get_aliased_columns(list(model_input.columns), model_alias, stmt.targets, mode='input')
    #     predictions.columns = get_aliased_columns(list(predictions.columns), model_alias, stmt.targets, mode='output')
    #
    #     if into:
    #         try:
    #             dtypes = {}
    #             for col in predictions.columns:
    #                 if model.dtype_dict.get(col, False):
    #                     dtypes[col] = self.dtypes_to_sql.get(col, sqlalchemy.Text)
    #
    #             data_handler.select_into(into, predictions, dtypes=dtypes)
    #         except Exception as e:
    #             print("Error when trying to store the JOIN output in data handler.")
    #
    #     r = HandlerResponse(
    #         RESPONSE_TYPE.TABLE,
    #         predictions
    #     )
    #     return r

    def _get_model(self, model_name):
        storage = self.storage.get('models')
        try:
            return dill.loads(storage[model_name]['model'])
        except KeyError:
            return None

    def _call_model(self, df, model):
        predictions = dask.compute(model.predict(df)[0])[0]
        target_name = model.config['output_features'][0]['column']
        if target_name not in df:
            predictions.columns = [target_name]
        else:
            predictions.columns = ['prediction']
        predictions[f'{target_name}_explain'] = None
        joined = df.join(predictions)
        return joined

    @mark_process(name='learn')
    def _learn(self, statement):
        model_name = statement.name.parts[-1]

        if model_name in self.get_tables().data_frame['table_name'].values:
            raise Exception("Error: this model already exists!")

        target = statement.targets[0].parts[-1]
        if statement.order_by:
            raise Exception("Ludwig handler does not support time series tasks yet!")

        # TODO: potentially abstract this into a common utility?
        # get data from integration  # TODO: custom dialect?
        integration_name = statement.integration_name.parts[0]
        query = Select(
            targets=[Star()],
            from_table=NativeQuery(
                integration=Identifier(integration_name),
                query=statement.query_str,
            )
        )
        sql_session = make_sql_session(self.company_id)
        sqlquery = SQLQuery(query, session=sql_session)
        df = sqlquery.fetch(view='dataframe')['result']
        user_config = {'hyperopt': {'executor': {'gpu_resources_per_trial': 0, 'num_samples': 3}}}  # no GPU for now

        # TODO: turn into common method?
        data_integration_meta = self.handler_controller.get(name=integration_name)
        ludwig_integration_meta = self.handler_controller.get(name='ludwig')
        predictor_record = db.Predictor(
            company_id=self.company_id,
            name=model_name,
            integration_id=ludwig_integration_meta['id'],
            data_integration_id=data_integration_meta['id'],
            fetch_data_query=statement.query_str,
            mindsdb_version=mindsdb_version,
            lightwood_version=ludwig_version,
            to_predict=target,
            learn_args=user_config,
            data={'name': model_name},
            training_data_columns_count=len(df.columns),
            training_data_rows_count=len(df),
            training_start_at=datetime.datetime.now(),
            status=PREDICTOR_STATUS.TRAINING
        )

        db.session.add(predictor_record)

        results = auto_train(
            dataset=df,
            target=target,
            # TODO: add and enable custom values via SQL (mindful of local vs cloud) for these params:
            tune_for_memory=False,
            time_limit_s=120,
            # output_directory='./',
            user_config=user_config,
            # random_seed=42,
            # use_reference_config=False,
            # kwargs={}
        )
        model = results.best_model

        all_models = self.storage.get('models')
        payload = {
            'stmt': statement,
            'model': dill.dumps(model),
        }
        if all_models is not None:
            all_models[model_name] = payload
        else:
            all_models = {model_name: payload}


        dtypes = {f['name']: f['type'] for f in model.base_config['input_features']}
        model_data = {
            'name': model_name,
            'status': 'complete',
            'dtype_dict': dtypes,
            'accuracies': {'metric': results.experiment_analysis.best_result['metric_score']}
        }
        all_metadata = self.storage.get('metadata')
        if all_metadata is not None:
            all_metadata[model_name] = model_data
        else:
            all_metadata = {model_name: model_data}

        # TODO: replace with new generic table
        integration_meta = self.handler_controller.get(name=integration_name)
        ludwig_integration_meta = self.handler_controller.get(name='ludwig')
        predictor_record = db.Predictor(
            company_id=self.company_id,
            name=model_name,
            integration_id=ludwig_integration_meta['id'],
            data_integration_id=integration_meta['id'],
            fetch_data_query=statement.query_str,
            mindsdb_version=mindsdb_version,
            lightwood_version=ludwig_version,
            to_predict=target,
            learn_args={},  # todo: match to user_config
            data=model_data,
            update_status='complete',
            training_data_columns_count=len(df.columns),
            training_data_rows_count=len(df),
            training_start_at=datetime.datetime.now()
        )

        db.session.add(predictor_record)
        db.session.commit()
        self.storage.set('metadata', all_metadata)
        self.storage.set('models', all_models)  # commits to internal storage once db record has been committed

        log.info(f'Ludwig model {model_name} has finished training.')

    @mark_process(name='predict')
    def predict(self, model_name, data):
        with RayConnection():
            if isinstance(data, dict):
                data = [data]
            df = pd.DataFrame(data)
            model = self._get_model(model_name)
            predictor_record = get_model_record(company_id=self.company_id, name=model_name, ml_handler_name='ludwig')
            target = predictor_record.to_predict[0]
            predictions = self._call_model(df, model)
            predictions = predictions.rename({
                target: f'{target}_original',
                'prediction': target
            }, axis=1)

            # TODO: convert in a common method to fill-in missing columns
            for col_name in ['select_data_query', 'when_data', f'{target}_confidence',
                             f'{target}_anomaly', f'{target}_min', f'{target}_max']:
                predictions[col_name] = None
            predictions = predictions.to_dict(orient='records')

            after_predict_hook(
                company_id=self.company_id,
                predictor_id=predictor_record.id,
                rows_in_count=df.shape[0],
                columns_in_count=df.shape[1],
                rows_out_count=len(predictions)
            )

            return predictions


class RayConnection:
    def __init__(self, addr=None, **kwargs):
        ray.init(address=addr, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        ray.shutdown()
