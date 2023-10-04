import os
import re
import sys
import shutil
import pickle
import subprocess
import traceback
from enum import Enum
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
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.integrations.utilities.utils import format_exception_error
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
import mindsdb.utilities.profiler as profiler

from .proc_wrapper import pd_decode, pd_encode, encode, decode
from .proc_wrapper import import_string, find_model_class


BYOM_TYPE = Enum('BYOM_TYPE', ['SAFE', 'UNSAFE'])


class BYOMHandler(BaseMLEngine):

    name = 'byom'

    def __init__(self, model_storage, engine_storage, **kwargs) -> None:
        # region check availability
        is_cloud = Config().get('cloud', False)
        if is_cloud is True:
            byom_enabled = os.environ.get('MINDSDB_BYOM_ENABLED', 'false').lower()
            if byom_enabled not in ('true', '1'):
                raise RuntimeError('BYOM is disabled on cloud')
        # endregion

        self.model_wrapper = None

        # region read MINDSDB_BYOM_TYPE
        try:
            self._byom_type = BYOM_TYPE[
                os.environ.get(
                    'MINDSDB_BYOM_TYPE',
                    BYOM_TYPE.UNSAFE.name
                ).upper()
            ]
        except KeyError:
            self._byom_type = BYOM_TYPE.SAFE
        # endregion

        super().__init__(model_storage, engine_storage, **kwargs)

    def _get_model_proxy(self):
        self.engine_storage.fileStorage.pull()
        code = self.engine_storage.fileStorage.file_get('code')
        modules_str = self.engine_storage.fileStorage.file_get('modules')

        if self.model_wrapper is None:
            if self._byom_type == BYOM_TYPE.UNSAFE:
                WrapperClass = ModelWrapperUnsafe
            elif self._byom_type == BYOM_TYPE.SAFE:
                WrapperClass = ModelWrapperSafe

            self.model_wrapper = WrapperClass(
                code=code,
                modules_str=modules_str,
                engine_id=self.engine_storage.integration_id
            )

        return self.model_wrapper

    def create(self, target, df=None, args=None, **kwargs):
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
        self.engine_storage.fileStorage.file_set(
            'code',
            Path(connection_args['code']).read_bytes()
        )

        self.engine_storage.fileStorage.file_set(
            'modules',
            Path(connection_args['modules']).read_bytes()
        )

        self.engine_storage.fileStorage.push()

        model_proxy = self._get_model_proxy()

        try:
            model_proxy.check()
        except Exception as e:
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
            model_state = model_proxy.finetune(df, model_state, args=args.get('using', {}))  # WRONG

            # region hack to speedup file saving
            with profiler.Context('finetune-byom-write-file'):
                dest_abs_path = self.model_storage.fileStorage.folder_path / 'model'
                with open(dest_abs_path, 'wb') as fd:
                    fd.write(model_state)
                self.model_storage.fileStorage.push(compression_level=0)
            # endregion

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


class ModelWrapperUnsafe:
    """ Model wrapper that executes learn/predict in current process
    """

    def __init__(self, code, modules_str, engine_id):
        module = import_string(code)
        model_class = find_model_class(module)
        self.model_class = model_class
        self.model_instance = self.model_class()

    def train(self, df, target, args):
        self.model_instance.train(df, target, args)
        return pickle.dumps(self.model_instance.__dict__, protocol=5)

    def predict(self, df, model_state, args):
        model_state = pickle.loads(model_state)
        self.model_instance.__dict__ = model_state
        try:
            result = self.model_instance.predict(df, args)
        except:
            result = self.model_instance.predict(df)
        return result

    def finetune(self, df, model_state, args):
        self.model_instance.__dict__ = pickle.loads(model_state)

        call_args = [df]
        if args:
            call_args.append(args)

        self.model_instance.finetune(df, args)

        return pickle.dumps(self.model_instance.__dict__, protocol=5)

    def check(self):
        pass


class ModelWrapperSafe:
    """ Model wrapper that executes learn/predict in venv
    """

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
            # DANGER !!! VENV MUST BE CREATED
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
            modules.append('pandas >=2.0.0, <2.1.0')

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