from mindsdb.libs.backends.ludwig import LudwigBackend
from mindsdb.libs.phases.base_module import BaseModule

import datetime


class ModelInterface(BaseModule):

    phase_name = PHASE_DATA_EXTRACTOR

    def run(self):
        if self.transaction.lmd['model_backend'] == 'ludwig':
            self.transaction.lmd['is_active'] = True
            self.transaction.model_backend = LudwigBackend(self.transaction)
            self.transaction.model_backend.train()
            self.transaction.lmd['is_active'] = False

        self.transaction.lmd['train_end_at'] = str(datetime.datetime.now())
