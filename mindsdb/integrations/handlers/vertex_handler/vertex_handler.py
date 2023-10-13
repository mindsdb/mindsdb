from mindsdb.integrations.libs.base import BaseMLEngine


class VertexHandler(BaseMLEngine):
    """Integration with the Nixtla Vertex library for
    time series forecasting with classical methods.
    """

    name = "Vertex"

    def create(self, target, df, args={}):
        pass

    def predict(self, df, args={}):
        pass