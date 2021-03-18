import os
import time
from threading import Thread
import walrus
from mindsdb.utilities.log import log
from mindsdb.integrations.base import Integration
from mindsdb.streams.redis.redis_stream import RedisStream
from mindsdb.interfaces.storage.db import session, Stream


class RedisConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port', 6379)
        self.database = kwargs.get('database', 0)
        self.user = kwargs.get('user', None)
        self.password = kwargs.get('password', None)

    def _get_connection(self):
        return walrus.Database(host=self.host, port=self.port,
                               db=self.database,
                               socket_connect_timeout=10)

    def check_connection(self):
        client = self._get_connection()
        try:
            client.dbsize()
            return True
        except Exception:
            return False


class Redis(Integration, Thread, RedisConnectionChecker):
    def __init__(self, config, name):
        Integration.__init__(self, config, name)
        intergration_info = self.config['integrations'][self.name]
        self.host = intergration_info.get('host')
        self.port = intergration_info.get('port', 6379)
        self.db = intergration_info.get('database', 0)
        self.input_stream = intergration_info.get('stream')
        self.user = intergration_info.get('user', None)
        self.password = intergration_info.get('password', None)
        self.client = self._get_connection()
        self.control_stream_name = self.integration_info.get('control_stream')
        self.control_stream = self.client.Stream(self.control_stream_name)
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.streams = {}
        Thread.__init__(self, target=Redis.work, args=(self, ))

    def setup(self):
        # read streams info from db
        existed_streams = session.query(Stream).filter_by(company_id=self.company_id, integration=self.name)
        for stream in existed_streams:
            kwargs = {"host": stream.host,
                      "port": stream.port,
                      "db": stream.db,
                      "type": stream._type,
                      "predictor": stream.predictor,
                      "stream_in": stream.stream_in,
                      "stream_out": stream.stream_out}
            to_launch = self.get_stream(**kwargs)
            to_launch.start()
            self.streams[stream.name] = to_launch


        self.start()

    def work(self):
        while True:
            time.sleep(5)
            #block==0 is a blocking mode
            recs = self.control_stream.read(block=0)
            for r in recs:
                binary_r = r[1]
                stream_params = self._decode(binary_r)
                stream = self.get_stream(**stream_params)
                stream.start()
                self.store_stream(stream)


    def store_stream(self, stream):
        stream_name = f"{self.name}_{stream.predictor}"
        stream_rec = Stream(name=stream_name, host=stream.host,
                            port=stream.port, db=stream.db,
                            _type=stream._type, predictor=stream.predictor,
                            integration=self.name, company_id=self.company_id,
                            stream_in=stream.stream_in, stream_out=stream.stream_out)
        session.add(stream_rec)
        session.commit()
        self.streams[stream_name] = stream

    def get_stream(self, **kwargs):
        stream_in = kwargs.get('input_stream')
        stream_out = kwargs.get('output_stream')
        predictor_name = kwargs.get('predictor')
        stream_type = kwargs.get('type', 'forecast')
        return RedisStream(self.host, self.port, self.db,
                           stream_in, stream_out, predictor_name,
                           stream_type)




    def _decode(self, b_dict):
        """convert binary key/value into strings"""
        decoded = {}
        if not isinstance(b_dict, dict):
            log.error(f"got unexpected data format from redis stream {self.name}: {b_dict}")
            return {}
        for k in b_dict:
            decoded[k.decode('utf8')] = b_dict[k].decode('utf8')
        return decoded

    def _query(self):
        pass

    def register_predictors(self, model_data_arr):
        pass

    def unregister_predictor(self, name):
        pass
