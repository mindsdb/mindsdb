from typing import Dict, Any

import os
import pandas as pd
from transformers import pipeline
from dateutil.parser import parse as parse_datetime
import dill
import shutil
import datetime

from transformers import __version__ as transformers_version

from mindsdb_sql import parse_sql
from mindsdb.utilities.log import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import mark_process
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.utilities.hooks import after_predict as after_predict_hook
import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.fs import LocalFSStore
from mindsdb import __version__ as mindsdb_version
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import BinaryOperation, Identifier, Constant, Select, Show, Star, NativeQuery
from mindsdb.integrations.utilities.utils import make_sql_session, get_where_data
from mindsdb.integrations.utilities.processes import HandlerProcess
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

# from .functions import learn_process


class HuggingFaceHandler(PredictiveHandler):

    name = 'huggingface'
    predictor_cache: Dict[str, Dict[str, Any]]

    def __init__(self, name, **kwargs):
        """ Handler to create and use Ludwig AutoML models from MindsDB. """  # noqa
        super().__init__(name)
        self.predictor_cache = {}
        self.config = Config()

        self.parser = parse_sql
        self.dialect = 'mindsdb'

        self.handler_controller = kwargs.get('handler_controller')
        self.storage = kwargs.get('fs_store')
        self.company_id = kwargs.get('company_id')
        self.model_controller = WithKWArgsWrapper(
            ModelController(),
            company_id=self.company_id
        )

    def check_connection(self, **kwargs) -> HandlerStatusResponse:
        """ Setup storage and check whether Ludwig is available. """  # noqa
        result = HandlerStatusResponse(False)
        try:
            from transformers import pipeline
            result.success = True
            self.is_connected = True
        except ImportError as e:
            log.error(f'Error importing Transformers, {e}!')
            result.error_message = str(e)
        return result

    def get_tables(self) -> HandlerResponse:
        """ Returns name list of trained models.  """  # noqa

        models = os.listdir(os.path.join(self.config['paths']['predictors']))  # TODO: replace with FSStore usage
        if models:
            hf_models = []
            for m in models:
                if 'huggingface_' in m:
                    hf_models.append(m)
            df = pd.DataFrame(hf_models, columns=['table_name'])
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
        if type(query) == CreatePredictor:
            self._learn(query)

        elif type(query) == RetrainPredictor:
            msg = 'Warning: retraining Ludwig models is not yet supported!'  # TODO
            return HandlerResponse(RESPONSE_TYPE.ERROR, error_message=msg)

        elif type(query) == DropPredictor:
            # @TODO: [REFACTOR] common helper method
            to_drop = query.name.parts[-1]
            models = self.get_tables().data_frame.values

            if models:
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
                    # self.storage.delete(f'huggingface_{self.company_id}_{predictor_record.id}')  # TODO use this once implemented
                    shutil.rmtree(os.path.join(self.config['paths']['predictors'], f'huggingface_{self.company_id}_{predictor_record.id}'))
                db.session.commit()
                # end common method

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

    def _get_model(self, model_name):
        storage = self.storage.get('models')
        try:
            return dill.loads(storage[model_name]['model'])
        except KeyError:
            return None

    def _call_model(self, df, model):
        predictions = pd.DataFrame()
        target_name = model.config['output_features'][0]['column']

        if target_name not in df:
            predictions.columns = [target_name]
        else:
            predictions.columns = ['prediction']

        predictions[f'{target_name}_explain'] = None
        joined = df.join(predictions)

        if 'prediction' in joined:
            joined = joined.rename({
                target_name: f'{target_name}_original',
                'prediction': target_name
            }, axis=1)
        return joined

    @mark_process(name='learn')
    def _learn(self, statement):
        model_name = statement.name.parts[-1]

        if model_name in self.get_tables().data_frame['table_name'].values:
            raise Exception("Error: this model already exists!")

        target = statement.targets[0].parts[-1]
        if statement.order_by:
            raise Exception("Ludwig handler does not support time series tasks yet!")

        user_config = statement.using
        data_integration_meta = self.handler_controller.get(name='files')
        huggingface_integration_meta = self.handler_controller.get(name='huggingface')
        predictor_record = db.Predictor(
            company_id=self.company_id,
            name=model_name,
            integration_id=huggingface_integration_meta['id'],
            data_integration_id=data_integration_meta['id'],
            fetch_data_query=statement.query_str,
            mindsdb_version=mindsdb_version,
            lightwood_version=transformers_version,
            to_predict=target,
            learn_args=user_config,
            data={'name': model_name},
            training_data_columns_count=0,
            training_data_rows_count=0,
            training_start_at=datetime.datetime.now(),
            status=PREDICTOR_STATUS.TRAINING
        )
        db.session.add(predictor_record)
        db.session.commit()
        predictor_id = predictor_record.id
        
        # create dummy dataframe
        # df = pd.DataFrame()

        # p = HandlerProcess(learn_process, df, target, user_config, predictor_id, statement, self.storage_config, self.storage_context)  # noqa
        # p.start()

        task = user_config['task'] 
        model_url = user_config['model_url']

        labels = user_config['labels']
        # candidate_labels = user_config['candidate_labels']
        # max_length = user_config['max_length']
        # min_output_length = user_config['min_output_length']
        # max_output_length = user_config['max_output_length']
        # lang_input = user_config['lang_input']
        # lang_output = user_config['lang_output']

        # REF:
        #   user_config.get(key, default_value) 

        base_dir = self.config['paths']['predictors']
        save_path = os.path.join(base_dir, f'huggingface_{self.company_id}_{predictor_id}')

        if task == 'translation':
            lang_input = 'en'  # TODO: change
            lang_output = 'es'  # TODO: change
            task_proper = f'translation_{lang_input}_to_{lang_output}'
        else:
            task_proper = task

        print(f'Downloading {model_url}...')
        model_pipeline = pipeline(task=task_proper, model=model_url)
        model_pipeline.save_pretrained(save_path)
        # self.storage.put(f'huggingface_{}_{model_}', models)
        
        if False:  # commented out as not needed for first example
            if max_length:
                max_length = max_length
            elif 'max_position_embeddings' in pipeline.model.config.to_dict().keys():
                max_length = pipeline.model.config.max_position_embeddings
            elif 'max_length' in pipeline.model.config.to_dict().keys():
                max_length = pipeline.model.config.max_length
            else:
                print('No max_length found!')

        if labels:
            labels_default = model_pipeline.model.config.id2label
            labels_map = {}
            for num in labels_default.keys():
                labels_map[labels_default[num]] = labels[num]
            labels_map = labels_map
        else:
            labels_map = None

        print(f'Saved to {save_path}')

        predictor_record = db.Predictor.query.with_for_update().get(predictor_id)
        predictor_record.status = PREDICTOR_STATUS.COMPLETE
        predictor_record.training_stop_at = datetime.datetime.now()
        db.session.commit()
        return HandlerResponse(RESPONSE_TYPE.OK)

    @mark_process(name='predict')
    def predict(self, model_name, data):
        if isinstance(data, dict):
            data = [data]
        df = pd.DataFrame(data)
        model = self._get_model(model_name)
        predictor_record = get_model_record(company_id=self.company_id, name=model_name, ml_handler_name='ludwig')
        target = predictor_record.to_predict[0]
        predictions = self._call_model(df, model)

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
