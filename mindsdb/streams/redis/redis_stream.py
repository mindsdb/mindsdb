import time
import json
from threading import Thread
import walrus
from mindsdb.utilities.log import log
from mindsdb_native import Predictor


class RedisStream(Thread):
    def __init__(self, host, port, database, input_stream, predictor):
        self.host = host
        self.port = port
        self.db = database
        self.predictor_name = predictor
        self.client = self._get_client()
        self.stream_in = self.client.Stream(input_stream)
        self.stream_out = self.client.Stream(predictor)
        super().__init__(target=RedisStream.make_prediction, args=(self,))

    def _get_client(self):
        return walrus.Database(host=self.host, port=self.port, db=self.db)

    def make_prediction(self):
        predictor = Predictor(self.predictor_name)
        while True:
            time.sleep(10)
            predict_info = self.stream_in.read()
            # log.error("got predict request(s): %s" % predict_info)
            for record in predict_info:
                record_id = record[0]
                raw_when_data = record[1]
                # log.error("raw when_data: %s" % raw_when_data)
                when_data = self.decode(raw_when_data)
                # log.error("when_data: %s" % when_data)

                # result = ca.mindsdb_native.predict(self.predictor_name, when_data=when_data)
                result = predictor.predict(when_data=when_data)
                # log.error("when_data: %s\tprediction: %s" % (when_data, result))
                for res in result:
                    # log.error("explainded: %s" % res.explain())
                    in_json = json.dumps(res.explain())
                    # log.error("json converted: %s" % in_json)
                    self.stream_out.add({"prediction": in_json})
                self.stream_in.delete(record_id)

    def decode(self, redis_data):
        decoded = {}
        for k in redis_data:
            decoded[k.decode('utf8')] = redis_data[k].decode('utf8')
        return decoded

    def encode(self, prediction_info):
        encoded = {}
        for k in prediction_info:
            log.error("%s: %s" % (k, type(k)))
            log.error(k.explain())

        for k in prediction_info:
            if isinstance(k, str):
                if isinstance(prediction_info[k], (list, tuple)):
                    if len(prediction_info[k]) == 1:
                        if not isinstance(prediction_info[k][0], (list, tuple)):
                            encoded[k] = prediction_info[k][0]
                        else:
                            encoded[k] = str(prediction_info[k][0])
                    else:
                        encoded[k] = str(prediction_info[k])
                else:
                    encoded[k] = prediction_info[k]

        return encoded
