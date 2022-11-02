import dill
import datetime

from ludwig.automl import auto_train

from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.integrations.utilities.utils import format_exception_error
from mindsdb.integrations.libs.storage_handler import SqliteStorageHandler
from mindsdb.utilities import log
import mindsdb.interfaces.storage.db as db

from .utils import RayConnection


def learn_process(df, target, user_config, predictor_id, sql_stmt, storage_config, storage_ctx):
    try:
        with RayConnection():
            results = auto_train(
                dataset=df,
                target=target,
                # TODO: enable custom values via SQL (mindful of local vs cloud) for these params
                tune_for_memory=False,
                time_limit_s=120,
                user_config=user_config,
                # output_directory='./',
                # random_seed=42,
                # use_reference_config=False,
                # kwargs={}
            )
        model = results.best_model
        model_name = sql_stmt.name.parts[-1]

        storage = SqliteStorageHandler(context=storage_ctx, config=storage_config)
        all_models = storage.get('models')
        payload = {
            'stmt': sql_stmt,
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

        # update internal storage
        all_metadata = storage.get('metadata')
        if all_metadata is not None:
            all_metadata[model_name] = model_data
        else:
            all_metadata = {model_name: model_data}

        storage.set('metadata', all_metadata)
        storage.set('models', all_models)

        predictor_record = db.Predictor.query.with_for_update().get(predictor_id)
        predictor_record.data = model_data
        predictor_record.training_stop_at = datetime.datetime.now()
        predictor_record.status = PREDICTOR_STATUS.COMPLETE
        db.session.commit()

        log.logger.info(f'Ludwig model {model_name} has finished training.')

    except Exception as e:
        predictor_record = db.Predictor.query.with_for_update().get(predictor_id)
        error_message = format_exception_error(e)
        predictor_record.data = {"error": error_message}
        predictor_record.status = PREDICTOR_STATUS.ERROR
        db.session.commit()
