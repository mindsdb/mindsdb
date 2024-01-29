import importlib

from pandas import DataFrame

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage
from mindsdb.integrations.libs.ml_handler_process.handlers_cacher import handlers_cacher
from mindsdb.utilities.functions import mark_process


@mark_process(name='learn')
def predict_process(integration_id: int, predictor_record: db.Predictor, args: dict,
                    module_path: str, ml_engine_name: str, dataframe: DataFrame) -> DataFrame:
    module = importlib.import_module(module_path)

    if predictor_record.id not in handlers_cacher:
        handlerStorage = HandlerStorage(integration_id)
        modelStorage = ModelStorage(predictor_record.id)
        ml_handler = module.Handler(
            engine_storage=handlerStorage,
            model_storage=modelStorage,
        )
        handlers_cacher[predictor_record.id] = ml_handler
    else:
        ml_handler = handlers_cacher[predictor_record.id]

    if ml_engine_name == 'lightwood':
        args['code'] = predictor_record.code
        args['target'] = predictor_record.to_predict[0]
        args['dtype_dict'] = predictor_record.dtype_dict
        args['learn_args'] = predictor_record.learn_args

    if ml_engine_name == 'langchain':
        from mindsdb.api.executor.controllers import SessionController
        from mindsdb.api.executor.command_executor import ExecuteCommands

        sql_session = SessionController()
        sql_session.database = 'mindsdb'

        command_executor = ExecuteCommands(sql_session)

        args['executor'] = command_executor

    predictions = ml_handler.predict(dataframe, args)
    ml_handler.close()
    return predictions
