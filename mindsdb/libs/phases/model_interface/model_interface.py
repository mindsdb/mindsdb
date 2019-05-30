from mindsdb.libs.backends.ludwig import LudwigBackend
from mindsdb.libs.phases.base_module import BaseModule


class ModelInterface(BaseModule):

    def run(self):
        if self.lmd['model_backend'] == 'ludwig':
            self.lmd['is_active'] = True
            self.model_backend = LudwigBackend(self)
            self.model_backend.train()
            self.lmd['is_active'] = False

        self.lmd['train_end_at'] = str(datetime.datetime.now())
