import sys
import os
import re
import shutil
import pickle
import subprocess
import traceback

from pathlib import Path
from datetime import datetime
from typing import Optional, Dict
from collections import OrderedDict

import numpy as np
import pandas as pd
from pandas.api import types as pd_types

from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.storage.fs import FileStorage, RESOURCE_GROUP
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.integrations.utilities.utils import format_exception_error
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from .proc_wrapper import pd_decode, pd_encode, encode, decode


class BYOMHandler(BaseMLEngine):

    name = 'byom'

    def _get_model_proxy(self):
        con_args = self.engine_storage.get_connection_args()
        code = self.engine_storage.file_get(con_args['code'])
        modules_str = self.engine_storage.file_get(con_args['modules'])

        return ModelWrapper(
            code=code,
            modules_str=modules_str,
            engine_id=self.engine_storage.integration_id
        )

    def create(self, target, df=None, args=None, **kwargs):
        is_cloud = Config().get('cloud', False)
        if is_cloud is True:
            raise RuntimeError('BYOM is disabled on cloud')

        model_proxy = self._get_model_proxy()

        model_state = model_proxy.train(df, target, args)

        self.model_storage.file_set('model', model_state)

        # TODO return columns?

        def convert_type(field_type):
            if pd_types.is_integer_dtype(field_type):
                return 'integer'
            elif pd_types.is_numeric_dtype(field_type):
                return 'float'
            elif pd_types.is_datetime64_any_dtype(field_type):
                return 'datetime'
            else:
                return 'categorical'

        columns = {
            target: convert_type(np.object)
        }

        self.model_storage.columns_set(columns)

    def predict(self, df, args=None):
        pred_args = args.get('predict_params', {})

        model_proxy = self._get_model_proxy()

        model_state = self.model_storage.file_get('model')

        pred_df = model_proxy.predict(df, model_state, pred_args)

        return pred_df

    def create_engine(self, connection_args):
        # check code and requirements

        model_proxy = self._get_model_proxy()

        try:
            model_proxy.check()
        except Exception as e:
            # remove venv
            model_proxy.remove_venv()

            raise e

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        model_storage = self.model_storage

        # TODO: should probably refactor at some point, as a bit of the logic is shared with lightwood's finetune logic
        try:
            base_predictor_id = args['base_model_id']
            base_predictor_record = db.Predictor.query.get(base_predictor_id)
            if base_predictor_record.status != PREDICTOR_STATUS.COMPLETE:
                raise Exception("Base model must be in status 'complete'")

            predictor_id = model_storage.predictor_id
            predictor_record = db.Predictor.query.get(predictor_id)

            predictor_record.data = {'training_log': 'training'} # TODO move to ModelStorage (don't work w/ db directly)
            predictor_record.training_start_at = datetime.now()
            predictor_record.status = PREDICTOR_STATUS.FINETUNING  # TODO: parallel execution block
            db.session.commit()

            model_proxy = self._get_model_proxy()
            model_state = self.base_model_storage.file_get('model')
            model_proxy.finetune(df, model_state, args=args.get('using', {}))
            self.model_storage.file_set('model', model_state)

            predictor_record.update_status = 'up_to_date'
            predictor_record.status = PREDICTOR_STATUS.COMPLETE
            predictor_record.training_stop_at = datetime.now()
            db.session.commit()

        except Exception as e:
            log.logger.error(e)
            predictor_id = model_storage.predictor_id
            predictor_record = db.Predictor.query.with_for_update().get(predictor_id)
            print(traceback.format_exc())
            error_message = format_exception_error(e)
            predictor_record.data = {"error": error_message}
            predictor_record.status = PREDICTOR_STATUS.ERROR
            db.session.commit()
            raise

        finally:
            if predictor_record.training_stop_at is None:
                predictor_record.training_stop_at = datetime.now()
                db.session.commit()

class ModelWrapper:
    def __init__(self, code, modules_str, engine_id):
        self.code = code
        modules = self.parse_requirements(modules_str)

        self.env_path = None
        self.prepare_env(modules, engine_id)

    def prepare_env(self, modules, engine_id):
        config = Config()

        try:
            import virtualenv

            base_path = config.get('byom', {}).get('venv_path')
            if base_path is None:
                # create in root path
                base_path = Path(config.paths['root']) / 'venvs'

            self.env_path = base_path / f'env_{engine_id}'

            self.python_path = self.env_path / 'bin' / 'python'

            if self.env_path.exists():
                # already exists. it means requirements are already installed
                return

            # create
            virtualenv.cli_run(['-p', sys.executable, str(self.env_path)])
            log.logger.info(f"Created new environment: {self.env_path}")

            if len(modules) > 0:
                self.install_modules(modules)

        except Exception as e:
            log.logger.info("Can't create virtual environment. venv module should be installed")

            self.python_path = Path(sys.executable)

            # try to install modules everytime
            self.install_modules(modules)

    def remove_venv(self):
        if self.env_path is not None and self.env_path.exists():
            shutil.rmtree(str(self.env_path))

    def parse_requirements(self, requirements):
        # get requirements from string
        # they should be located at the top of the file, before code

        pattern = '^[\w\\[\\]-]+[=!<>\s]*[\d\.]*[,=!<>\s]*[\d\.]*$'
        modules = []
        for line in requirements.split(b'\n'):
            line = line.decode().strip()
            if line:
                if re.match(pattern, line):
                    modules.append(line)
                else:
                    raise Exception(f'Wrong requirement: {line}')

        is_pandas = any([m.lower().startswith('pandas') for m in modules])
        if not is_pandas:
            modules.append('pandas >=1.1.5,<=1.3.3')

        # for dataframe serialization
        modules.append('pyarrow==11.0.0')
        return modules

    def install_modules(self, modules):
        # install in current environment using pip

        pip_cmd = self.python_path.parent / 'pip'
        for module in modules:
            p = subprocess.Popen([pip_cmd, 'install', module], stderr=subprocess.PIPE)
            p.wait()
            if p.returncode != 0:
                raise Exception(f'Problem with installing module {module}: {p.stderr.read()}')

    def _run_command(self, params):
        params_enc = encode(params)

        wrapper_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'proc_wrapper.py')
        p = subprocess.Popen(
            [str(self.python_path), wrapper_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        p.stdin.write(params_enc)
        p.stdin.close()
        ret_enc = p.stdout.read()

        p.wait()

        try:
            ret = decode(ret_enc)
        except (pickle.UnpicklingError, EOFError):
            raise RuntimeError(p.stderr.read())
        return ret

    def check(self):

        params = {
            'method': 'check',
            'code': self.code,
        }
        return self._run_command(params)

    def train(self, df, target, args):
        params = {
            'method': 'train',
            'df': pd_encode(df),
            'code': self.code,
            'to_predict': target,
            'args': args,
        }

        model_state = self._run_command(params)
        return model_state

    def predict(self, df, model_state, args):

        params = {
            'method': 'predict',
            'code': self.code,
            'df': pd_encode(df),
            'model_state': model_state,
            'args': args,
        }
        pred_df = self._run_command(params)
        return pd_decode(pred_df)

    def finetune(self, df, model_state, args):
        params = {
            'method': 'finetune',
            'model_state': model_state,
            'df': pd_encode(df),
            'code': self.code,
            'args': args,
        }

        model_state = self._run_command(params)
        return model_state


connection_args = OrderedDict(
    code={
        'type': ARG_TYPE.PATH,
        'description': 'The path to model code'
    },
    modules={
        'type': ARG_TYPE.PATH,
        'description': 'The path to model requirements'
    }
)