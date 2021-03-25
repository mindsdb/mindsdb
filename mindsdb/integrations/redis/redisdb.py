import os
import json
from threading import Thread
import walrus
from mindsdb.utilities.config import STOP_THREADS_EVENT
from mindsdb.utilities.log import log
from mindsdb.integrations.base import Integration
from mindsdb.streams.redis.redis_stream import RedisStream
from mindsdb.interfaces.storage.db import session, Stream, Configuration


class RedisConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port', 6379)
        self.db = kwargs.get('db', 0)
        self.user = kwargs.get('user', None)
        self.password = kwargs.get('password', None)

    def _get_connection(self):
        return walrus.Database(host=self.host,
                               port=self.port,
                               db=self.db,
                               socket_connect_timeout=10)

    def check_connection(self):
        client = self._get_connection()
        try:
            client.dbsize()
            return True
        except Exception:
            return False


class Redis(Integration, RedisConnectionChecker):
    """Redis Integration which is more a Streams factory
    than classical Integration."""
    def __init__(self, config, name):
        Integration.__init__(self, config, name)
        intergration_info = self.config['integrations'][self.name]
        self.host = intergration_info.get('host')
        self.port = intergration_info.get('port', 6379)
        self.db = intergration_info.get('db', 0)
        self.control_stream_name = intergration_info.get('stream')
        self.user = intergration_info.get('user', None)
        self.password = intergration_info.get('password', None)
        self.client = self._get_connection()
        self.control_stream = self.client.Stream(self.control_stream_name)
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.streams = {}
        self.stop_event = STOP_THREADS_EVENT

    def should_i_exist(self):
        config_record = session.query(Configuration).filter_by(company_id=self.company_id).first()
        if config_record is None:
            return False
        integrations = json.loads(config_record.data)["integrations"]
        if self.name not in integrations:
            return False
        return True

    def setup(self):
        """Launches streams stored in db and
        Launches a worker in separate thread which waits
        data from control stream and creates a particular Streams."""

        self.start_stored_streams()
        # launch worker which reads control steam and spawn streams
        self.start()

    def start(self):
        Thread(target=Redis.work, args=(self, )).start()

    def start_stored_streams(self):
        existed_streams = session.query(Stream).filter_by(company_id=self.company_id, integration=self.name)

        for stream in existed_streams:
            to_launch = self.get_stream_from_db(stream)
            if stream.name not in self.streams:
                params = {"integration": stream.integration,
                          "predictor": stream.predictor,
                          "stream_in": stream.stream_in,
                          "stream_out": stream.stream_out,
                          "type": stream._type}

                log.debug(f"Integration {self.name} - launching from db : {params}")
                to_launch.start()
                self.streams[stream.name] = to_launch.stop_event

    def delete_stream(self, predictor):
        """Deletes stream from database and stops it work by
        setting up a special threading.Event flag."""
        stream_name = f"{self.name}_{predictor}"
        log.debug(f"Integration {self.name}: deleting {stream_name}")
        session.query(Stream).filter_by(company_id=self.company_id, integration=self.name, name=stream_name).delete()
        session.commit()
        if stream_name in self.streams:
            self.streams[stream_name].set()

    def work(self):
        """Creates a Streams by receiving initial information from control stream."""
        log.debug(f"Integration {self.name}: start listening {self.control_stream_name} redis stream")
        while not self.stop_event.wait(0.5):
            try:
                # break if no record about this integration has found in db
                if not self.should_i_exist():
                    break
                # First, lets check that there are no new records in db, created via HTTP API for e.g.
                self.start_stored_streams()
                # Checking new incoming records(requests) for new stream.
                # Could't use blocking reading here, because this loop must
                # also check new records in db (created via HTTP api for e.g)
                recs = self.control_stream.read()
                for r in recs:
                    r_id = r[0]
                    binary_r = r[1]
                    stream_params = self._decode(binary_r)
                    if "input_stream" not in stream_params or "output_stream" not in stream_params:
                        self.delete_stream(stream_params['predictor'])
                        self.control_stream.delete(r_id)
                        continue

                    stream = self.get_stream_from_kwargs(**stream_params)
                    log.debug(f"Integration {self.name}: creating stream: {stream_params}")
                    stream.start()
                    # store created stream in database
                    self.store_stream(stream)
                    # need to delete record which steam is already created
                    self.control_stream.delete(r_id)
            except Exception as e:
                log.error(f"Integration {self.name}: {e}")

        # received exit event
        log.debug(f"Integration {self.name}: exiting...")
        self.stop_streams()
        session.close()

    def stop_streams(self):
        for stream in self.streams:
            self.streams[stream].set()

    def store_stream(self, stream):
        """Stories a created stream."""
        stream_name = f"{self.name}_{stream.predictor}"
        stream_rec = Stream(name=stream_name, host=stream.host,
                            port=stream.port, db=stream.db,
                            _type=stream._type, predictor=stream.predictor,
                            integration=self.name, company_id=self.company_id,
                            stream_in=stream.stream_in_name, stream_out=stream.stream_out_name)
        session.add(stream_rec)
        session.commit()
        self.streams[stream_name] = stream.stop_event

    def get_stream_from_db(self, db_record):
        kwargs = {"type": db_record._type,
                  "predictor": db_record.predictor,
                  "input_stream": db_record.stream_in,
                  "output_stream": db_record.stream_out}
        return self.get_stream_from_kwargs(**kwargs)

    def get_stream_from_kwargs(self, **kwargs):
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
            log.error(f"Integration {self.name}: got unexpected data format from redis control stream {self.control_stream_name}: {b_dict}")
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
