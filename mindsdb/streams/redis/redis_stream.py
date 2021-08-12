import json
import walrus

from mindsdb.streams.base import BaseStream
from mindsdb.api.http.initialize import CustomJSONEncoder

class RedisStream(BaseStream):
    def __init__(self, stream, connection_info):
        self.client = walrus.Database(**connection_info)
        self.stream = self.client.Stream(stream)

    @staticmethod
    def _decode(redis_data):
        decoded = {}
        for k in redis_data:
            decoded[k.decode('utf8')] = redis_data[k].decode('utf8')
        return decoded

    def read(self):
        for k, when_data in self.stream.read():
            print(f'READING: {when_data} - FROM {self.stream.info} - WITH CONSUMER: {self.stream.consumers_info}')
            try:
                res = json.loads(when_data[b''])
            except KeyError:
                res = self._decode(when_data)
            yield res
            self.stream.delete(k)

    def write(self, dct):
        print(f'WRTING: {dct} - TO {self.stream.info} - WITH CONSUMER: {self.stream.consumers_info}')
        try:
            dump = json.dumps(dct)
        except:
            dump = json.dumps(json.dumps({'y': 1, 'x1': 1, 'x2': 2}))
        self.stream.add({'': dump})
