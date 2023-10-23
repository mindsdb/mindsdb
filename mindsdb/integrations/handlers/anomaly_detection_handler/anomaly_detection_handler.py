from mindsdb.integrations.libs.base import BaseMLEngine


class AnomalyDetectionHandler(BaseMLEngine):
    """Integration with the PyOD and CatBoost libraries for
    anomaly detection. Both supervised and unsupervised.
    """

    name = "anomaly_detection"

    def create(self):
        pass

    def predict(self):
        pass
