import os
import logging
import tempfile
from pathlib import Path
import torch.multiprocessing as mp
from lightwood.api import predictor
from lightwood.api.high_level import predictor_from_code
from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.interfaces.storage.db import session, Predictor
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.config import Config
from mindsdb.utilities.fs import create_process_mark, delete_process_mark
import mindsdb.interfaces.storage.db as db
import pandas as pd
import torch
import gc
import lightwood


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

def run_learn(preidctor_id: int, df: pd.DataFrame) -> None:
    create_process_mark('learn')

    predictor_record = session.query(db.Predictor).filter_by(id=preidctor_id).first()
    assert predictor_record is not None

    fs_store = FsSotre()
    config = Config()
    
    predictor: lightwood.PredictorInterface = lightwood.predictor_from_code(predictor_record.code)
    predictor.learn(df)

    predictor_record = session.query(db.Predictor).filter_by(id=preidctor_id).first()
    assert predictor_record is not None

    save_name = f'{predictor_record.company_id}@@@@@{predictor_record.name}'
    pickle_path = os.path.join(config['paths']['predictors'], save_name)
    predictor.save(pickle_path)

    fs_store.put(save_name, save_name, config['paths']['predictors'])

    predictor_record.data = predictor.model_analysis.to_dict()  # type: ignore
    predictor_record.dtype_dict = predictor.dtype_dict  # type: ignore
    session.commit()
    delete_process_mark('learn')


class LearnProcess(ctx.Process):
    daemon = True

    def __init__(self, *args):
        super(LearnProcess, self).__init__(args=args)

    def run(self):
        '''
        running at subprocess due to
        ValueError: signal only works in main thread

        this is work for celery worker here?
        '''
        run_learn(*self._args)
