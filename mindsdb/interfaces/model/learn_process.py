import os
import traceback
import tempfile
from pathlib import Path

import pandas as pd
from pandas.core.frame import DataFrame
import torch.multiprocessing as mp
import lightwood
from lightwood.api.types import ProblemDefinition
from lightwood import __version__ as lightwood_version

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.interfaces.model.model_interface import ModelInterface, ModelInterfaceWrapper
from mindsdb.interfaces.storage.db import session, Predictor
from mindsdb import __version__ as mindsdb_version
from mindsdb.interfaces.datastore.datastore import DataStore, DataStoreWrapper
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.utilities.config import Config
from mindsdb.utilities.functions import mark_process
from mindsdb.utilities.log import log


ctx = mp.get_context('spawn')


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
def run_generate(df: DataFrame, problem_definition: ProblemDefinition, name: str, company_id: int, datasource_id: int) -> int:
    json_ai = lightwood.json_ai_from_problem(df, problem_definition)
    code = lightwood.code_from_json_ai(json_ai)

    predictor_record = db.Predictor(
        company_id=company_id,
        name=name,
        json_ai=json_ai.to_dict(),
        code=code,
        datasource_id=datasource_id,
        mindsdb_version=mindsdb_version,
        lightwood_version=lightwood_version,
        to_predict=[problem_definition.target],
        learn_args=problem_definition.to_dict(),
        data={'name': name}
    )

    db.session.add(predictor_record)
    db.session.commit()


@mark_process(name='learn')
def run_fit(predictor_id: int, df: pd.DataFrame) -> None:
    try:
        predictor_record = session.query(db.Predictor).filter_by(id=predictor_id).first()
        assert predictor_record is not None

        fs_store = FsStore()
        config = Config()

        predictor_record.data = {'training_log': 'training'}
        session.commit()
        predictor: lightwood.PredictorInterface = lightwood.predictor_from_code(predictor_record.code)
        predictor.learn(df)

        session.refresh(predictor_record)

        fs_name = f'predictor_{predictor_record.company_id}_{predictor_record.id}'
        pickle_path = os.path.join(config['paths']['predictors'], fs_name)
        predictor.save(pickle_path)

        fs_store.put(fs_name, fs_name, config['paths']['predictors'])

        predictor_record.data = predictor.model_analysis.to_dict()
        predictor_record.dtype_dict = predictor.dtype_dict
        session.commit()

        dbw = DatabaseWrapper(predictor_record.company_id)
        mi = ModelInterfaceWrapper(ModelInterface(), predictor_record.company_id)
        dbw.register_predictors([mi.get_model_data(predictor_record.name)])
    except Exception as e:
        session.refresh(predictor_record)
        predictor_record.data = {'error': f'{traceback.format_exc()}\nMain error: {e}'}
        session.commit()
        raise e


def run_learn(df: DataFrame, problem_definition: ProblemDefinition, name: str, company_id: int, datasource_id: int) -> None:
    run_generate(df, problem_definition, name, company_id, datasource_id)
    predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
    assert predictor_record is not None
    run_fit(predictor_record.id, df)


def run_adjust(name, db_name, from_data, datasource_id, company_id):
    # @TODO: Actually implement this
    return 0


def run_update(name: str, company_id: int):
    original_name = name
    name = f'{company_id}@@@@@{name}'

    fs_store = FsStore()
    config = Config()
    data_store = DataStoreWrapper(DataStore(), company_id)

    try:
        predictor_record = Predictor.query.filter_by(company_id=company_id, name=original_name).first()
        assert predictor_record is not None

        predictor_record.update_status = 'updating'

        session.commit()
        ds = data_store.get_datasource_obj(None, raw=False, id=predictor_record.datasource_id)
        df = ds.df

        problem_definition = predictor_record.learn_args

        problem_definition['target'] = predictor_record.to_predict[0]

        if 'join_learn_process' in problem_definition:
            del problem_definition['join_learn_process']

        # Adapt kwargs to problem definition
        if 'timeseries_settings' in problem_definition:
            problem_definition['timeseries_settings'] = problem_definition['timeseries_settings']

        if 'stop_training_in_x_seconds' in problem_definition:
            problem_definition['time_aim'] = problem_definition['stop_training_in_x_seconds']

        json_ai = lightwood.json_ai_from_problem(df, problem_definition)
        predictor_record.json_ai = json_ai.to_dict()
        predictor_record.code = lightwood.code_from_json_ai(json_ai)
        predictor_record.data = {'training_log': 'training'}
        session.commit()
        predictor: lightwood.PredictorInterface = lightwood.predictor_from_code(predictor_record.code)
        predictor.learn(df)

        fs_name = f'predictor_{predictor_record.company_id}_{predictor_record.id}'
        pickle_path = os.path.join(config['paths']['predictors'], fs_name)
        predictor.save(pickle_path)
        fs_store.put(fs_name, fs_name, config['paths']['predictors'])
        predictor_record.data = predictor.model_analysis.to_dict()  # type: ignore
        session.commit()

        predictor_record.lightwood_version = lightwood.__version__
        predictor_record.mindsdb_version = mindsdb_version
        predictor_record.update_status = 'up_to_date'
        session.commit()

    except Exception as e:
        log.error(e)
        predictor_record.update_status = 'update_failed'  # type: ignore
        session.commit()
        return str(e)


class LearnProcess(ctx.Process):
    daemon = True

    def __init__(self, *args):
        super(LearnProcess, self).__init__(args=args)

    def run(self):
        run_learn(*self._args)


class GenerateProcess(ctx.Process):
    daemon = True

    def __init__(self, *args):
        super(GenerateProcess, self).__init__(args=args)

    def run(self):
        run_generate(*self._args)


class FitProcess(ctx.Process):
    daemon = True

    def __init__(self, *args):
        super(FitProcess, self).__init__(args=args)

    def run(self):
        run_fit(*self._args)


class AdjustProcess(ctx.Process):
    daemon = True

    def __init__(self, *args):
        super(AdjustProcess, self).__init__(args=args)

    def run(self):
        '''
        running at subprocess due to
        ValueError: signal only works in main thread

        this is work for celery worker here?
        '''
        run_adjust(*self._args)


class UpdateProcess(ctx.Process):
    daemon = True

    def __init__(self, *args):
        super(UpdateProcess, self).__init__(args=args)

    def run(self):
        run_update(*self._args)
