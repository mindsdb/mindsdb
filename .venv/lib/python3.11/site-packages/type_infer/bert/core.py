from type_infer.base import BaseEngine


class BERType(BaseEngine):
    def __init__(self, stable=False):
        super().__init__(stable=stable)

    def infer(self, df):
        raise NotImplementedError
