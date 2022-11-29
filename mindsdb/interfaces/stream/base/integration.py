from threading import Thread

from mindsdb.interfaces.stream.utilities import STOP_THREADS_EVENT
from mindsdb.utilities import log
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.context import context as ctx


class Integration:
    def __init__(self, config, name):
        self.config = config
        self.name = name
        self.mindsdb_database = config['api']['mysql']['database']
        self.company_id = ctx.company_id

    def setup(self):
        raise NotImplementedError

    def _query(self, query, fetch=False):
        raise NotImplementedError


class StreamIntegration(Integration):
    def __init__(self, config, name, control_stream=None):
        Integration.__init__(self, config, name)
        self._streams = []
        self._control_stream = control_stream

    def setup(self):
        Thread(target=StreamIntegration._loop, args=(self,)).start()

    def _loop(self):
        log.logger.info("INTEGRATION %s: starting", self.name)
        while not STOP_THREADS_EVENT.wait(1.0):
            if self._control_stream is not None:
                # Create or delete streams based on messages from control_stream
                for dct in self._control_stream.read():
                    if 'action' not in dct:
                        log.logger.error('INTEGRATION %s: no action value found in control record - %s', self.name, dct)
                    else:
                        if dct['action'] == 'create':
                            for k in ['name', 'predictor', 'stream_in', 'stream_out']:
                                if k not in dct:
                                    # Not all required parameters were provided (i.e. stream will not be created)
                                    # TODO: what's a good way to notify user about this?
                                    log.logger.error('INTEGRATION %s: stream creating error. not enough data in control record - %s', self.name, dct)
                                    break
                            else:
                                log.logger.info('INTEGRATION %s: creating stream %s', self.name, dct['name'])
                                if db.session.query(db.Stream).filter_by(name=dct['name'], company_id=self.company_id).first() is None:
                                    stream = db.Stream(
                                        company_id=self.company_id,
                                        name=dct['name'],
                                        integration=self.name,
                                        predictor=dct['predictor'],
                                        stream_in=dct['stream_in'],
                                        stream_out=dct['stream_out'],
                                        anomaly_stream=dct.get('stream_anomaly', None),
                                        learning_params=dct.get('learning_params', None),
                                        learning_threshold=dct.get('learning_threshold', None),
                                    )
                                    db.session.add(stream)
                                    db.session.commit()
                                else:
                                    log.logger.error('INTEGRATION %s: stream with this name already exists - %s', self.name, dct['name'])
                        elif dct['action'] == 'delete':
                            for k in ['name']:
                                if k not in dct:
                                    # Not all required parameters were provided (i.e. stream will not be created)
                                    # TODO: what's a good way to notify user about this?
                                    log.logger.error('INTEGRATION %s: unable to delete stream - stream name is not provided', self.name)
                                    break
                            else:
                                log.logger.error('INTEGRATION %s: deleting stream - %s', self.name, dct['name'])
                                db.session.query(db.Stream).filter_by(
                                    company_id=self.company_id,
                                    integration=self.name,
                                    name=dct['name']
                                ).delete()
                                db.session.commit()
                        else:
                            # Bad action value
                            log.logger.error('INTEGRATION %s: bad action value received - %s', self.name, dct)

            # it is really required to add 'commit' here
            # to avoid case when sessin.query returns previously
            # deleted object
            # https://stackoverflow.com/questions/10210080/how-to-disable-sqlalchemy-caching
            db.session.commit()
            stream_db_recs = db.session.query(db.Stream).filter_by(
                company_id=self.company_id,
                integration=self.name
            ).all()

            # Stop streams that weren't found in DB
            indices_to_delete = []
            for i, s in enumerate(self._streams):
                if s.name not in map(lambda x: x.name, stream_db_recs):
                    log.logger.info("INTEGRATION %s: stopping stream - %s", self.name, s.name)
                    indices_to_delete.append(i)
                    self._streams[i].stop_event.set()
            self._streams = [s for i, s in enumerate(self._streams) if i not in indices_to_delete]

            # Start new streams found in DB
            for s in stream_db_recs:
                if s.name not in map(lambda x: x.name, self._streams):
                    log.logger.info("INTEGRATION %s: starting stream - %s", self.name, s.name)
                    self._streams.append(self._make_stream(s))

        log.logger.info("INTEGRATION %s: stopping", self.name)
        for s in self._streams:
            s.stop_event.set()

    def _make_stream(self, s: db.Stream):
        raise NotImplementedError

    def _query(self, query, fetch=False):
        pass
