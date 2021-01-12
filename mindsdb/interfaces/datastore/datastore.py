import json
import datetime
from dateutil.parser import parse as parse_dt
import shutil
import os
import pickle
from mindsdb.utilities.log import log as logger

from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb_native import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS
from mindsdb.interfaces.state.state import State
from mindsdb.interfaces.state.config import Config


class DataStore():
    def __init__(self, config):
        self.config = Config(config)
        self.dir = config['paths']['datasources']
        self.state = State(self.config)
        self.mindsdb_native = MindsdbNative(config)

    def get_analysis(self, name):
        datasource = self.state.get_datasource(name)
        if datasource.analysis is not None:
            return json.loads(datasource.analysis)

        ds = self.get_datasource_obj(name)
        analysis = self.mindsdb_native.analyse_dataset(ds)
        self.state.update_datasource(name=name, analysis=json.dumps(analysis))
        return analysis


    def _datasource_to_dict(self, datasource):
        metadata = json.loads(datasource.data) if datasource.data is not None else {}
        datasource_dict = {}
        datasource_dict['name'] = datasource.name
        datasource_dict['source_type'] = metadata.get('source_type', None)
        datasource_dict['source'] = metadata.get('source', None)
        datasource_dict['row_count'] = metadata.get('row_count', None)
        datasource_dict['columns'] = metadata.get('columns', None)
        datasource_dict['created_at'] = datasource.created_at
        datasource_dict['updated_at'] = datasource.modified_at
        return datasource_dict

    def get_datasources(self):
        datasource_arr = []
        for datasource in self.state.list_datasources():
            try:
                datasource_dict = self._datasource_to_dict(datasource)
                datasource_arr.append(datasource_dict)
            except Exception as e:
                print(e)
        return datasource_arr

    def get_data(self, name, where=None, limit=None, offset=None):
        if offset is None:
            offset = 0

        ds = self.get_datasource_obj(name)

        # @TODO Remove and add `offset` to the `filter` method of the datasource
        if limit is not None:
            filtered_ds = ds.filter(where=where, limit=limit+offset)
        else:
            filtered_ds = ds.filter(where=where)

        filtered_ds = filtered_ds.iloc[offset:]

        data = filtered_ds.to_dict(orient='records')
        return {
            'data': data,
            'rowcount': len(ds),
            'columns_names': filtered_ds.columns
        }

    def get_datasource(self, name):
        datasource = self.state.get_datasource(name)
        if datasource is None:
            return None
        datasource_dict = self._datasource_to_dict(datasource)
        return datasource_dict

    def delete_datasource(self, name):
        self.state.delete_datasource(name)
        try:
            shutil.rmtree(os.path.join(self.dir, name))
        except Exception as e:
            pass

    def save_datasource(self, name, source_type, source, file_path=None):
        if source_type == 'file' and (file_path is None):
            raise Exception('`file_path` argument required when source_type == "file"')

        for i in range(1, 1000):
            if name in [x['name'] for x in self.get_datasources()]:
                previous_index = i - 1
                name = name.replace(f'__{previous_index}__', '')
                name = f'{name}__{i}__'
            else:
                break

        ds_meta_dir = os.path.join(self.dir, name)
        os.mkdir(ds_meta_dir)

        try:
            if source_type == 'file':
                source = os.path.join(ds_meta_dir, source)
                shutil.move(file_path, source)
                ds = FileDS(source)

                picklable = {
                    'class': 'FileDS',
                    'args': [source],
                    'kwargs': {}
                }

            elif source_type in self.config['integrations']:
                integration = self.config['integrations'][source_type]

                ds_class_map = {
                    'clickhouse': ClickhouseDS,
                    'mariadb': MariaDS,
                    'mysql': MySqlDS,
                    'postgres': PostgresDS,
                    'mssql': MSSQLDS,
                    'mongodb': MongoDS,
                    'snowflake': SnowflakeDS
                }

                try:
                    dsClass = ds_class_map[integration['type']]
                except KeyError:
                    raise KeyError(f"Unknown DS type: {source_type}, type is {integration['type']}")

                if integration['type'] in ['clickhouse']:
                    picklable = {
                        'class': dsClass.__name__,
                        'args': [],
                        'kwargs': {
                            'query': source['query'],
                            'user': integration['user'],
                            'password': integration['password'],
                            'host': integration['host'],
                            'port': integration['port']
                        }
                    }
                    ds = dsClass(**picklable['kwargs'])

                elif integration['type'] in ['mssql', 'postgres', 'mariadb', 'mysql']:
                    picklable = {
                        'class': dsClass.__name__,
                        'args': [],
                        'kwargs': {
                            'query': source['query'],
                            'user': integration['user'],
                            'password': integration['password'],
                            'host': integration['host'],
                            'port': integration['port']
                        }
                    }

                    if 'database' in integration:
                        picklable['kwargs']['database'] = integration['database']

                    if 'database' in source:
                        picklable['kwargs']['database'] = source['database']

                    ds = dsClass(**picklable['kwargs'])

                elif integration['type'] == 'snowflake':
                    picklable = {
                        'class': dsClass.__name__,
                        'args': [],
                        'kwargs': {
                            'query': source['query'],
                            'schema': source['schema'],
                            'warehouse': source['warehouse'],
                            'database': source['database'],
                            'host': integration['host'],
                            'password': integration['password'],
                            'user': integration['user'],
                            'account': integration['account']
                        }
                    }

                    ds = dsClass(**picklable['kwargs'])

                elif integration['type'] == 'mongodb':
                    picklable = {
                        'class': dsClass.__name__,
                        'args': [],
                        'kwargs': {
                            'database': source['database'],
                            'collection': source['collection'],
                            'query': source['find'],
                            'user': integration['user'],
                            'password': integration['password'],
                            'host': integration['host'],
                            'port': integration['port']
                        }
                    }

                    ds = dsClass(**picklable['kwargs'])
            else:
                # This probably only happens for urls
                ds = FileDS(source)
                picklable = {
                    'class': 'FileDS',
                    'args': [source],
                    'kwargs': {}
                }

            df = ds.df

            if '' in df.columns or len(df.columns) != len(set(df.columns)):
                shutil.rmtree(ds_meta_dir)
                raise Exception('Each column in datasource must have unique name')

            with open(os.path.join(ds_meta_dir, 'ds.pickle'), 'wb') as fp:
                pickle.dump(picklable, fp)

        except Exception as e:
            logger.error(f'Can\'t create datasource {name} due to exception: "{e}" | Will proceede to delet it\'s directory: {ds_meta_dir}')
            if os.path.isdir(ds_meta_dir):
                shutil.rmtree(ds_meta_dir)
            raise e

        meta = {
            'name': name,
            'source_type': source_type,
            'source': source,
            'row_count': len(df),
            'columns': [dict(name=x) for x in list(df.keys())]
        }

        self.state.make_datasource(name=name, analysis=None, storage_path=ds_meta_dir, data=json.dumps(meta))
        return self.get_datasource_obj(name, raw=True), name

    def get_datasource_obj(self, name, raw=False):
        self.state.load_datasource(name)
        ds_meta_dir = os.path.join(self.dir, name)
        ds = None
        try:
            with open(os.path.join(ds_meta_dir, 'ds.pickle'), 'rb') as fp:
                picklable = pickle.load(fp)
                if raw:
                    return picklable
                try:
                    ds = eval(picklable['class'])(*picklable['args'], **picklable['kwargs'])
                except Exception:
                    ds = picklable
            return ds
        except Exception as e:
            print(f'\n{e}\n')
            return None
