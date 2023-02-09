import sys
import os
import re
import pickle
import subprocess
from collections import OrderedDict
from pathlib import Path

import numpy as np
from pandas.api import types as pd_types

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

from .proc_wrapper import pd_decode, pd_encode, encode, decode


class BYOMHandler(BaseMLEngine):

    name = 'byom'

    def _get_model_code(self):
        # TODO :
        file_name = self.engine_storage.get_connection_args()['model_code']
        return self.engine_storage.file_get(file_name)

    def create(self, target, df=None, args=None, **kwargs):
        is_cloud = Config().get('cloud', False)
        if is_cloud is True:
            raise RuntimeError('BYOM is disabled on cloud')

        model_proxy = ModelWrapper(
            model_code=self._get_model_code(),
            model_id=self.engine_storage.integration_id
        )

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
        model_proxy = ModelWrapper(
            model_code=self._get_model_code(),
            model_id=self.engine_storage.integration_id
        )

        model_state = self.model_storage.file_get('model')

        pred_df = model_proxy.predict(df, model_state)

        # rename target column
        # target = self.model_storage.get_info()['to_predict'][0]
        # pred_df = pred_df.rename(columns={target: 'prediction'})
        return pred_df


class ModelWrapper:
    def __init__(self, model_code, model_id):
        self.model_code = model_code
        requirements = self.find_requirements()

        self.prepare_env(model_id, requirements)

    def prepare_env(self, model_id, requirements):
        config = Config()

        try:
            import virtualenv

            base_path = config.get('byom', {}).get('venv_path')
            if base_path is None:
                # create in root path
                base_path = Path(config.paths['root']) / 'venvs'

            env_path = base_path / f'env_{model_id}'

            self.python_path = env_path / 'bin' / 'python'

            if env_path.exists():
                # already exists. it means requirements are already installed
                return

            # create
            virtualenv.cli_run(['-p', sys.executable, str(env_path)])
            log.logger.info(f"Created new environment: {env_path}")

            if len(requirements) > 0:
                self.install_modules(requirements)

        except Exception as e:
            log.logger.info("Can't create virtual environment. venv module should be installed")

            self.python_path = Path(sys.executable)

            # try to install modules everytime
            self.install_modules(requirements)

    def find_requirements(self):
        # get requirements from string
        # they should be located at the top of the file, before code

        pattern = '^[\w\\[\\]-]+[=!<>\s]*[\d\.]*[,=!<>\s]*[\d\.]*$'
        modules = []
        for line in self.model_code.split(b'\n'):
            line = line.decode().strip()
            if line.startswith('#'):
                module = line.lstrip('# ')
                if re.match(pattern, module):
                    modules.append(module)
            elif line != '':
                # it's code. exiting
                break

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
            os.system(f'{pip_cmd} install {module}')

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

    def train(self, df, target):
        params = {
            'method': 'train',
            'df': pd_encode(df),
            'code': self.model_code,
            'to_predict': target
        }

        model_state = self._run_command(params)
        return model_state

    def predict(self, df, model_state):

        params = {
            'method': 'predict',
            'code': self.model_code,
            'df': pd_encode(df),
            'model_state': model_state,
        }
        pred_df = self._run_command(params)
        return pd_decode(pred_df)


connection_args = OrderedDict(
    model_code={
        'type': ARG_TYPE.PATH,
        'description': 'The path name to model code'
    }
)