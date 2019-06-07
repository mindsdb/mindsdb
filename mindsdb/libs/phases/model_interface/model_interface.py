from mindsdb.libs.backends.ludwig import LudwigBackend
from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.constants.mindsdb import *

import datetime


class ModelInterface(BaseModule):

    phase_name = PHASE_MODEL_INTERFACE

    def run(self, mode='train'):
        if self.transaction.lmd['model_backend'] == 'ludwig':
            if mode == 'train':
                self.transaction.lmd['is_active'] = True
                self.transaction.model_backend = LudwigBackend(self.transaction)
                self.transaction.model_backend.train()
                self.transaction.lmd['is_active'] = False
                self.transaction.lmd['train_end_at'] = str(datetime.datetime.now())
            elif mode == 'predict':
                self.transaction.model_backend = LudwigBackend(self.transaction)
                self.transaction.hmd['predictions'] = self.transaction.model_backend.predict()

        if self.transaction.lmd['model_backend'] == 'lightwood':
            if mode == 'train':
                self.transaction.lmd['is_active'] = True
                self.transaction.model_backend = LudwigBackend(self.transaction)
                self.transaction.model_backend.train()
                self.transaction.lmd['is_active'] = False
                self.transaction.lmd['train_end_at'] = str(datetime.datetime.now())
            elif mode == 'predict':
                raise Exception('Predict not implemented for lightwood')    
