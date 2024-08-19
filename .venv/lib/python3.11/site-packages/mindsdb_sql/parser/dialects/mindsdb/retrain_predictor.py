from .create_predictor import CreatePredictorBase


class RetrainPredictor(CreatePredictorBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._action = 'RETRAIN'
        self._object = ''
