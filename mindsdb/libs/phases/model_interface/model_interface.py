from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.constants.mindsdb import *

import datetime


class ModelInterface(BaseModule):
    def run(self, mode='train'):
        try:
            from mindsdb.libs.backends.ludwig import LudwigBackend
        except ImportError as e:
            # Ludwig is optional, so this is fine
            pass

        try:
            from mindsdb.libs.backends.lightwood import LightwoodBackend
        except ImportError as e:
            self.log.warning(e)

        if self.transaction.hmd['model_backend'] == 'ludwig':
            self.transaction.model_backend = LudwigBackend(self.transaction)
        elif self.transaction.hmd['model_backend'] == 'lightwood':
            self.transaction.model_backend = LightwoodBackend(self.transaction)
        else:
            self.transaction.model_backend = self.transaction.hmd['model_backend']

        if hasattr(self.transaction.model_backend, 'set_transaction'):
            self.transaction.model_backend.set_transaction(self.transaction)

        if mode == 'train':
            self.transaction.model_backend.train()
            self.transaction.lmd['train_end_at'] = str(datetime.datetime.now())
        elif mode == 'predict':
            self.transaction.hmd['predictions'] = self.transaction.model_backend.predict()
