import os
import json
from threading import Thread, Event
import walrus
from mindsdb.utilities.log import log
from mindsdb_native import Predictor
from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Predictor as DBPredictor


class RedisStream(Thread):
    def __init__(self, host, port, database, stream_in, stream_out, predictor, _type):
        self.host = host
        self.port = port
        self.db = database
        self.predictor = predictor
        self.client = self._get_client()
        self.stream_in_name = stream_in
        self.stream_out_name = stream_out
        self.stream_in = self.client.Stream(stream_in)
        self.stream_out = self.client.Stream(stream_out)
        self._type = _type

        self.stop_event = Event()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        super().__init__(target=RedisStream.make_prediction, args=(self,))

    def _get_client(self):
        return walrus.Database(host=self.host, port=self.port, db=self.db)

    def make_prediction(self):
        predict_record = session.query(DBPredictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if predict_record is None:
            log.error(f"Error creating stream: requested predictor {self.predictor} is not exist")
            return

        predictor = Predictor(self.predictor)
        while not self.stop_event.wait(0.5):
            # block==0 is a blocking mode
            predict_info = self.stream_in.read(block=0)
            for record in predict_info:
                record_id = record[0]
                raw_when_data = record[1]
                when_data = self.decode(raw_when_data)

                result = predictor.predict(when_data=when_data)
                for res in result:
                    in_json = json.dumps(res.explain())
                    self.stream_out.add({"prediction": in_json})
                self.stream_in.delete(record_id)

    def decode(self, redis_data):
        decoded = {}
        for k in redis_data:
            decoded[k.decode('utf8')] = redis_data[k].decode('utf8')
        return decoded
