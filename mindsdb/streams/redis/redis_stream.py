import json

import walrus

from mindsdb.streams.base import BaseStream
from mindsdb.utilities.json_encoder import CustomJSONEncoder


class RedisStream(BaseStream):
    def __init__(self, stream, connection_info):
        if isinstance(connection_info, str):
            connection_info = json.loads(connection_info)
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
            try:
                res = json.loads(when_data[b''])
            except KeyError:
                res = self._decode(when_data)
            yield res
            self.stream.delete(k)

    def write(self, dct):
        self.stream.add({'': json.dumps(dct, cls=CustomJSONEncoder)})
