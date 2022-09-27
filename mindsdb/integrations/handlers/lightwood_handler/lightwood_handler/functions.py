import os
import sys
import math
import traceback
import tempfile
from pathlib import Path
import json
import requests
from datetime import datetime

import pandas as pd
from pandas.core.frame import DataFrame
import lightwood
from lightwood.api.types import ProblemDefinition, JsonAI

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.db import session, Predictor
from mindsdb.utilities.functions import mark_process
from mindsdb.utilities.log import log
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.integrations.utilities.utils import format_exception_error
from mindsdb.interfaces.model.functions import (
    get_model_record,
    get_model_records
)
from mindsdb.interfaces.storage.fs import FileStorage, RESOURCE_GROUP

from .utils import rep_recur, brack_to_mod


def create_learn_mark():
    if os.name == 'posix':
        p = Path(tempfile.gettempdir()).joinpath('mindsdb/learn_processes/')
        p.mkdir(parents=True, exist_ok=True)
        p.joinpath(f'{os.getpid()}').touch()


def delete_learn_mark():
    if os.name == 'posix':
        p = Path(tempfile.gettempdir()).joinpath('mindsdb/learn_processes/').joinpath(f'{os.getpid()}')
        if p.exists():
            p.unlink()


@mark_process(name='learn')
def run_generate(df: DataFrame, problem_definition: ProblemDefinition, predictor_id: int, json_ai_override: dict = None):
    json_ai = lightwood.json_ai_from_problem(df, problem_definition)
    if json_ai_override is None:
        json_ai_override = {}

    json_ai_override = brack_to_mod(json_ai_override)
    json_ai = json_ai.to_dict()
    rep_recur(json_ai, json_ai_override)
    json_ai = JsonAI.from_dict(json_ai)

    code = lightwood.code_from_json_ai(json_ai)

    predictor_record = Predictor.query.with_for_update().get(predictor_id)
    predictor_record.json_ai = json_ai.to_dict()
    predictor_record.code = code
    db.session.commit()


@mark_process(name='learn')
def run_fit(predictor_id: int, df: pd.DataFrame, company_id: int) -> None:
    try:
        predictor_record = Predictor.query.with_for_update().get(predictor_id)
        assert predictor_record is not None

        predictor_record.data = {'training_log': 'training'}
        predictor_record.status = PREDICTOR_STATUS.TRAINING
        db.session.commit()
        predictor: lightwood.PredictorInterface = lightwood.predictor_from_code(predictor_record.code)
        predictor.learn(df)

        db.session.refresh(predictor_record)

        fs = FileStorage(
            resource_group=RESOURCE_GROUP.PREDICTOR,
            resource_id=predictor_id,
            company_id=company_id,
            sync=True
        )
        predictor.save(fs.folder_path)
        fs.push()

        predictor_record.data = predictor.model_analysis.to_dict()

        # getting training time for each tried model. it is possible to do
        # after training only
        fit_mixers = list(predictor.runtime_log[x] for x in predictor.runtime_log
                          if isinstance(x, tuple) and x[0] == "fit_mixer")
        submodel_data = predictor_record.data.get("submodel_data", [])
        # add training time to other mixers info
        if submodel_data and fit_mixers and len(submodel_data) == len(fit_mixers):
            for i, tr_time in enumerate(fit_mixers):
                submodel_data[i]["training_time"] = tr_time
        predictor_record.data["submodel_data"] = submodel_data

        predictor_record.dtype_dict = predictor.dtype_dict
        db.session.commit()
    except Exception as e:
        db.session.refresh(predictor_record)
        predictor_record.data = {'error': f'{traceback.format_exc()}\nMain error: {e}'}
        db.session.commit()
        raise e


@mark_process(name='learn')
def run_learn_remote(df: DataFrame, predictor_id: int) -> None:
    try:
        serialized_df = json.dumps(df.to_dict())
        predictor_record = Predictor.query.with_for_update().get(predictor_id)
        resp = requests.post(predictor_record.data['train_url'],
                             json={'df': serialized_df, 'target': predictor_record.to_predict[0]})

        assert resp.status_code == 200
        predictor_record.data['status'] = 'complete'
    except Exception:
        predictor_record.data['status'] = 'error'
        predictor_record.data['error'] = str(resp.text)

    session.commit()


@mark_process(name='learn')
def run_learn(df: DataFrame, problem_definition: ProblemDefinition, predictor_id: int,
              json_ai_override: dict = None, company_id: int = None) -> None:
    if json_ai_override is None:
        json_ai_override = {}

    predictor_record = Predictor.query.with_for_update().get(predictor_id)
    predictor_record.training_start_at = datetime.now()
    db.session.commit()

    try:
        run_generate(df, problem_definition, predictor_id, json_ai_override)
        run_fit(predictor_id, df, company_id)
    except Exception as e:
        predictor_record = Predictor.query.with_for_update().get(predictor_id)
        print(traceback.format_exc())

        error_message = format_exception_error(e)

        predictor_record.data = {"error": error_message}
        predictor_record.status = PREDICTOR_STATUS.ERROR
        db.session.commit()

    predictor_record.training_stop_at = datetime.now()
    predictor_record.status = PREDICTOR_STATUS.COMPLETE
    db.session.commit()


def run_adjust(name, db_name, from_data, datasource_id, company_id):
    # @TODO: Actually implement this
    return 0


@mark_process(name='learn')
def run_update(predictor_id: int, df: DataFrame, company_id: int, storage_path: str):
    try:
        predictor_record = Predictor.query.filter_by(id=predictor_id).first()

        problem_definition = predictor_record.learn_args
        problem_definition['target'] = predictor_record.to_predict[0]

        if 'join_learn_process' in problem_definition:
            del problem_definition['join_learn_process']

        if 'stop_training_in_x_seconds' in problem_definition:
            problem_definition['time_aim'] = problem_definition['stop_training_in_x_seconds']

        json_ai = lightwood.json_ai_from_problem(df, problem_definition)
        predictor_record.json_ai = json_ai.to_dict()
        predictor_record.code = lightwood.code_from_json_ai(json_ai)
        predictor_record.data = {'training_log': 'training'}
        predictor_record.training_start_at = datetime.now()
        predictor_record.status = PREDICTOR_STATUS.TRAINING
        session.commit()
        predictor: lightwood.PredictorInterface = lightwood.predictor_from_code(predictor_record.code)
        predictor.learn(df)

        fs_name = f'predictor_{predictor_record.company_id}_{predictor_record.id}'
        pickle_path = os.path.join(storage_path, fs_name)
        predictor.save(pickle_path)
        predictor_record.data = predictor.model_analysis.to_dict()
        predictor_record.update_status = 'up_to_date'
        predictor_record.dtype_dict = predictor.dtype_dict

        predictor_record.status = PREDICTOR_STATUS.COMPLETE
        predictor_record.training_stop_at = datetime.now()
        session.commit()

        predictor_records = get_model_records(
            active=None,
            name=predictor_record.name,
            company_id=company_id
        )
        predictor_records = [
            x for x in predictor_records
            if x.training_stop_at is not None
        ]
        predictor_records.sort(key=lambda x: x.training_stop_at)
        for record in predictor_records:
            record.active = False
        predictor_records[-1].active = True
        session.commit()
    except Exception as e:
        log.error(e)
        predictor_record = Predictor.query.with_for_update().get(predictor_id)
        print(traceback.format_exc())

        error_message = format_exception_error(e)

        predictor_record.data = {"error": error_message}

        # old_predictor_record.update_status = 'update_failed'   # TODO
        db.session.commit()

    if predictor_record.training_stop_at is None:
        predictor_record.training_stop_at = datetime.now()
        db.session.commit()
