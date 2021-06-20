from mindsdb.streams import StreamController
import os
from threading import Thread

from mindsdb.utilities.config import STOP_THREADS_EVENT
import mindsdb.interfaces.storage.db as db


class Integration:
    def __init__(self, config, name):
        self.config = config
        self.name = name
        self.mindsdb_database = config['api']['mysql']['database']
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)

    def setup(self):
        raise NotImplementedError

    def _query(self, query, fetch=False):
        raise NotImplementedError

    def register_predictors(self, model_data_arr):
        raise NotImplementedError

    def unregister_predictor(self, name):
        raise NotImplementedError


class StreamIntegration(Integration):
    def __init__(self, config, name, control_stream=None):
        Integration.__init__(self, config, name)
        self._streams = []
        self._control_stream = control_stream
    
    def setup(self):
        Thread(target=StreamIntegration._loop, args=(self,)).start()

    def _loop(self):
        while not STOP_THREADS_EVENT.wait(1.0):
            if self._control_stream is not None:
                # Create or delete streams based on messages from control_stream
                for dct in self._control_stream.read():
                    if 'action' not in dct:
                        pass
                    else:
                        if dct['action'] == 'create':
                            for k in ['name', 'predictor', 'stream_in', 'stream_out']:
                                if k not in dct:
                                    # Not all required parameters were provided (i.e. stream will not be created)
                                    # TODO: what's a good way to notify user about this?
                                    break
                            else:
                                stream = db.Stream(
                                    company_id=self.company_id,
                                    name=dct['name'],
                                    integration=self.name,
                                    predictor=dct['predictor'],
                                    stream_in=dct['stream_in'],
                                    stream_out=dct['stream_out'],
                                    anomaly_stream=dct.get('anomaly_stream', None),
                                    learning_stream=dct.get('learning_stream', None)
                                )
                                db.session.add(stream)
                                db.session.commit()

                        elif dct['action'] == 'delete':
                            for k in ['name']:
                                if k not in dct:
                                    # Not all required parameters were provided (i.e. stream will not be created)
                                    # TODO: what's a good way to notify user about this?
                                    break
                            else:
                                s = db.session.query(db.Stream).filter_by(
                                    company_id=self.company_id,
                                    integration=self.name,
                                    name=dct['name']
                                ).first()
                                if s is not None:
                                    s.delete()
                                db.session.commit()
                        else:
                            # Bad action value
                            pass
                
            stream_db_recs = db.session.query(db.Stream).filter_by(
                company_id=self.company_id,
                integration=self.name
            ).all()

            # Stop streams that weren't found in DB
            indices_to_delete = []
            for i, s in enumerate(self._streams):
                if s.name not in map(lambda x: x.name, stream_db_recs):
                    indices_to_delete.append(i)
                    self._streams[i].stop_event.set()
            self._streams = [s for i, s in enumerate(self._streams) if i not in indices_to_delete]

            # Start new streams found in DB
            for s in stream_db_recs:
                if s.name not in map(lambda x: x.name, self._streams):
                    self._streams.append(self._make_stream(s))

        for s in self._streams:
            s.stop_event.set()

    def _make_stream(self, s: db.Stream) -> StreamController:
        raise NotImplementedError

    def _query(self, query, fetch=False):
        pass

    def register_predictors(self, model_data_arr):
        pass

    def unregister_predictor(self, name):
        pass
