import sys
import os
import re
import pickle
import subprocess
from collections import OrderedDict
from pathlib import Path
import shutil

import numpy as np
from pandas.api import types as pd_types

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

from .proc_wrapper import pd_decode, pd_encode, encode, decode


class BYOMHandler(BaseMLEngine):

    name = 'byom'

    def _get_model_proxy(self):
        code = self.engine_storage.file_get('code')
        modules_str = self.engine_storage.file_get('modules')

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

        model_state = model_proxy.train(df, target)

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

        model_proxy = self._get_model_proxy()

        model_state = self.model_storage.file_get('model')

        pred_df = model_proxy.predict(df, model_state)

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

    def train(self, df, target):
        params = {
            'method': 'train',
            'df': pd_encode(df),
            'code': self.code,
            'to_predict': target
        }

        model_state = self._run_command(params)
        return model_state

    def predict(self, df, model_state):

        params = {
            'method': 'predict',
            'code': self.code,
            'df': pd_encode(df),
            'model_state': model_state,
        }
        pred_df = self._run_command(params)
        return pd_decode(pred_df)


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