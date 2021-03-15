from threading import Thread
import walrus
from flask import current_app as ca


class RedisStream(Thread):
    def __init__(self, host, port, database, input_stream, predictor):
        self.host = host
        self.port = port
        self.db = database
        self.predictor = predictor
        self.client = self._get_client()
        self.stream_in = self.client.Stream(input_stream)
        self.stream_out = self.client.Stream(predictor)
        super().__init__(target=RedisStream.make_prediction, args=(self,))

    def _get_client(self):
        return walrus.Database(host=self.host, port=self.port, db=self.db)
    
    def make_prediction(self):
        while True:
            predict_info = self.stream_in.read()
            for record in predict_info:
                when_data = record[1]
                result = ca.mindsdb_native.predict(self.predictor, when_data=when_data)
                self.stream_out.add({'prediction': result})
