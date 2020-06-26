# Mindsdb native interface
import mindsdb_native
from dateutil.parser import parse as parse_datetime

from mindsdb.interfaces.native.predictor_process import PredictorProcess


class MindsdbNative():
    def __init__(self, config):
        self.config = config
        self.metapredictor = mindsdb_native.Predictor('metapredictor')
        self.unregister_from = []

        try:
            assert(config['integrations']['default_clickhouse']['enabled'] == True)
            from mindsdb.interfaces.clickhouse.clickhouse import Clickhouse
            clickhouse = Clickhouse(self.config)
            self.unregister_from.append(clickhouse)
        except Exception as e:
            print(e)
            pass

        try:
            assert(config['integrations']['default_mariadb']['enabled'] == True)
            from mindsdb.interfaces.mariadb.mariadb import Mariadb
            mariadb = Mariadb(self.config)
            self.unregister_from.append(mariadb)
        except Exception as e:
            print(e)
            pass

    def learn(self, name, from_data, to_predict, kwargs={}):
        p = PredictorProcess(name, from_data, to_predict, kwargs, self.config.get_all(), 'learn')
        p.start()

    def predict(self, name, when=None, when_data=None, kwargs={}):
        # @TODO Separate into two paths, one for "normal" predictions and one for "real time" predictions. Use the multiprocessing code commented out bellow for normal (once we figure out how to return the prediction object... else use the inline code but with the "real time" predict functionality of mindsdb_native taht will be implemented later)
        '''
        from_data = when if when is not None else when_data
        p = PredictorProcess(name, from_data, to_predict=None, kwargs=kwargs, config=self.config.get_all(), 'predict')
        p.start()
        predictions = p.join()
        '''
        mdb = mindsdb_native.Predictor(name=name)

        kwargs['use_gpu'] = self.config.get('use_gpu', None)

        if when is not None:
            predictions = mdb.predict(
                when=when,
                run_confidence_variation_analysis=True,
                **kwargs
            )
        else:
            predictions = mdb.predict(
                when_data=when_data,
                run_confidence_variation_analysis=False,
                **kwargs
            )

        return predictions

    def analyse_dataset(self, ds):
        return self.metapredictor.analyse_dataset(ds, sample_margin_of_error=0.025)

    def get_model_data(self, name):
        return self.metapredictor.get_model_data(name)

    def get_models(self, status='any'):
        models = self.metapredictor.get_models()
        models = [x for x in models if x['name'] != 'metapredictor']
        if status != 'any':
            models = [x for x in models if x['status'] == status]

        for i in range(len(models)):
            for k in ['train_end_at', 'updated_at', 'created_at']:
                if k in models[i] and models[i][k] is not None:
                    try:
                        models[i][k] = parse_datetime(str(models[i][k]).split('.')[0])
                    except Exception as e:
                        models[i][k] = parse_datetime(str(models[i][k]))
        return models

    def delete_model(self, name):
        self.metapredictor.delete_model(name)
        for entity in self.unregister_from:
            unregister_func = getattr(entity, 'unregister_predictor')
            unregister_func(name)

    def rename_model(self, name, new_name):
        self.metapredictor.rename_model(name, new_name)

    def load_model(self, fpath):
        self.metapredictor.load_model(model_archive_path=fpath)

    def export_model(self,name):
        self.metapredictor.export_model(model_name=name)
