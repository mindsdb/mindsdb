import os
import json
import kafka

from threading import Thread
from mindsdb.utilities.config import STOP_THREADS_EVENT
from mindsdb.utilities.log import log
from mindsdb.integrations.base import Integration
from mindsdb.streams.kafka.kafka_stream import KafkaStream
from mindsdb.interfaces.storage.db import session, Stream, Configuration

class KafkaConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port', 9092)

    def _get_connection(self):
        return kafka.KafkaAdminClient(bootstrap_servers=f"{self.host}:{self.port}")
    def check_connection(self):
        try:
            client = self._get_connection()
            client.close()
            return True
        except Exception:
            return False


class Kafka(Integration, KafkaConnectionChecker):
    def __init__(self, config, name):
        Integration.__init__(self, config, name)
        intergration_info = self.config['integrations'][self.name]
        self.host = intergration_info.get('host')
        self.port = intergration_info.get('port', 9092)
        self.control_topic_name = intergration_info.get('topic')
        self.client = self._get_connection()
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
        Thread(target=Kafka.work, args=(self, )).start()

    def stop_deleted_streams(self):
        existed_streams = session.query(Stream).filter_by(company_id=self.company_id, integration=self.name)
        actual_streams = [x.name for x in existed_streams]

        for stream in self.streams:
            if stream not in actual_streams:
                # this stream is still running but it has been deleted from database.
                # need to stop it.
                self.streams[stream].set()

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

                log.error(f"Integration {self.name} - launching from db : {params}")
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
            del self.streams[stream_name]

    def work(self):
        self.consumer = kafka.KafkaConsumer(bootstrap_servers=f"{self.host}:{self.port}",
                                            consumer_timeout_ms=1000)

        self.consumer.subscribe([self.control_topic_name])
        log.error(f"Integration {self.name}: subscribed  to {self.control_topic_name} kafka topic")
        while not self.stop_event.wait(0.5):
            try:
                # break if no record about this integration has found in db
                if not self.should_i_exist():
                    break
                self.start_stored_streams()
                self.stop_deleted_streams()
                try:
                    msg_str = next(self.consumer)
                except StopIteration:
                    continue

                    stream_params = json.loads(msg_str.value)
                    stream = self.get_stream_from_kwargs(**stream_params)
                    stream.start()
                    # store created stream in database
                    self.store_stream(stream)
            except Exception as e:
                log.error(f"Integration {self.name}: {e}")

        # received exit event
        self.consumer.close()
        self.stop_streams()
        session.close()
        log.error(f"Integration {self.name}: exiting...")

    def store_stream(self, stream):
        """Stories a created stream."""
        stream_name = f"{self.name}_{stream.predictor}"
        stream_rec = Stream(name=stream_name, host=stream.host, port=stream.port,
                            _type=stream._type, predictor=stream.predictor,
                            integration=self.name, company_id=self.company_id,
                            stream_in=stream.stream_in_name, stream_out=stream.stream_out_name)
        session.add(stream_rec)
        session.commit()
        self.streams[stream_name] = stream.stop_event

    def stop_streams(self):
        for stream in self.streams:
            self.streams[stream].set()

    def get_stream_from_db(self, db_record):
        kwargs = {"type": db_record._type,
                  "predictor": db_record.predictor,
                  "input_stream": db_record.stream_in,
                  "output_stream": db_record.stream_out}
        return self.get_stream_from_kwargs(**kwargs)

    def get_stream_from_kwargs(self, **kwargs):
        topic_in = kwargs.get('input_stream')
        topic_out = kwargs.get('output_stream')
        predictor_name = kwargs.get('predictor')
        stream_type = kwargs.get('type', 'forecast')
        return KafkaStream(self.host, self.port,
                           topic_in, topic_out,
                           predictor_name, stream_type)


    def _query(self):
        pass

    def register_predictors(self, model_data_arr):
        pass

    def unregister_predictor(self, name):
        pass
