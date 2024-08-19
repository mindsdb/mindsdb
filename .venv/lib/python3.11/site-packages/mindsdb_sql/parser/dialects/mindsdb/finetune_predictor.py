from .create_predictor import CreatePredictorBase


class FinetunePredictor(CreatePredictorBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._action = 'FINETUNE'
        self._object = ''
