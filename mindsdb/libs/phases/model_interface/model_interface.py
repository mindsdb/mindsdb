try:
    from mindsdb.libs.backends.ludwig import LudwigBackend
except ImportError as e:
    print(e)
    pass

try:
    from mindsdb.libs.backends.lightwood import LightwoodBackend
except ImportError as e:
    print(e)
    pass

from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.constants.mindsdb import *

import datetime


class ModelInterface(BaseModule):

    phase_name = PHASE_MODEL_INTERFACE

    def run(self, mode='train'):
        if self.transaction.lmd['model_backend'] == 'ludwig':
            self.transaction.model_backend = LudwigBackend(self.transaction)
        if self.transaction.lmd['model_backend'] == 'lightwood':
            self.transaction.model_backend = LightwoodBackend(self.transaction)

        if mode == 'train':
            self.transaction.model_backend.train()
            self.transaction.lmd['train_end_at'] = str(datetime.datetime.now())
        elif mode == 'predict':
            self.transaction.hmd['predictions'] = self.transaction.model_backend.predict()
