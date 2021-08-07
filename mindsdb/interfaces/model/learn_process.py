import os
import tempfile
from pathlib import Path
from pandas.core.frame import DataFrame
import torch.multiprocessing as mp
from lightwood.api.types import ProblemDefinition
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.interfaces.model.model_interface import ModelInterface, ModelInterfaceWrapper
from mindsdb.interfaces.storage.db import session, Predictor
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.config import Config
from mindsdb.utilities.fs import create_process_mark, delete_process_mark
import mindsdb.interfaces.storage.db as db
import pandas as pd
import torch
import gc
import lightwood
from mindsdb import __version__ as mindsdb_version
from lightwood import __version__ as lightwood_version


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


def run_generate(df: DataFrame, problem_definition: ProblemDefinition, name: str, company_id: int, datasource_id: int) -> int:
    create_process_mark('learn')
    try:
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
            data={'status': 'untrained', 'name': name}
        )

        db.session.add(predictor_record)
        db.session.commit()
        session.refresh(predictor_record)
        return predictor_record.id
    finally:
        delete_process_mark('learn')


def run_fit(preidctor_id: int, df: pd.DataFrame) -> None:
    create_process_mark('learn')
    try:
        predictor_record = session.query(db.Predictor).filter_by(id=preidctor_id).first()
        assert predictor_record is not None

        fs_store = FsSotre()
        config = Config()

        predictor: lightwood.PredictorInterface = lightwood.predictor_from_code(predictor_record.code)
        predictor.learn(df)

        session.refresh(predictor_record)

        fs_name = f'predictor_{predictor_record.company_id}_{predictor_record.id}'
        pickle_path = os.path.join(config['paths']['predictors'], fs_name)
        predictor.save(pickle_path)

        fs_store.put(fs_name, fs_name, config['paths']['predictors'])

        predictor_record.data = predictor.model_analysis.to_dict()  # type: ignore
        predictor_record.data['status'] = 'complete'  # type: ignore
        predictor_record.data['name'] = predictor_record.name  # type: ignore
        predictor_record.dtype_dict = predictor.dtype_dict  # type: ignore
        session.commit()

        dbw = DatabaseWrapper(predictor_record.company_id)
        mi = ModelInterfaceWrapper(ModelInterface(), predictor_record.company_id)
        dbw.register_predictors([mi.get_model_data(predictor_record.name)])
    except Exception as e:
        session.refresh(predictor_record)
        predictor_record.data = {'status': 'error', 'name': predictor_record.name}
        session.commit()
        raise e
    finally:
        delete_process_mark('learn')


def run_learn(df: DataFrame, problem_definition: ProblemDefinition, name: str, company_id: int, datasource_id: int) -> None:
    predicotr_id = run_generate(df, problem_definition, name, company_id, datasource_id)
    run_learn(predicotr_id, df)


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
