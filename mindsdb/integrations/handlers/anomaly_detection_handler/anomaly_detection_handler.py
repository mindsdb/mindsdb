from mindsdb.integrations.libs.base import BaseMLEngine
from pyod.models.ecod import ECOD  # unsupervised default
from pyod.models.xgbod import XGBOD  # semi-supervised default
from catboost import CatBoostClassifier  # supervised default


def choose_model(df, supervised_threshold=3000):
    if len(df) > supervised_threshold:
        return CatBoostClassifier()
    else:
        return XGBOD()


class AnomalyDetectionHandler(BaseMLEngine):
    """Integration with the PyOD and CatBoost libraries for
    anomaly detection. Both supervised and unsupervised.
    """

    name = "anomaly_detection"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    def create(self):
        pass

    def predict(self):
        pass
