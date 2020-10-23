import json
import datetime
from dateutil.parser import parse as parse_dt
import shutil
import os
import pickle

from mindsdb.interfaces.datastore.sqlite_helpers import get_sqlite_data, cast_df_columns_types, create_sqlite_db
from mindsdb.interfaces.native.mindsdb import MindsdbNative
from mindsdb_native import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS



class DataStore():
    def __init__(self, config):
        self.config = config
        self.dir = config.paths['datasources']
        self.mindsdb_native = MindsdbNative(config)

    def get_analysis(self, ds):
        if isinstance(ds, str):
            return self.mindsdb_native.analyse_dataset(self.get_datasource_obj(ds))
        else:
            return self.mindsdb_native.analyse_dataset(ds)

    def get_datasources(self):
        datasource_arr = []
        for ds_name in os.listdir(self.dir):
            try:
                with open(os.path.join(self.dir, ds_name, 'metadata.json'), 'r') as fp:
                    try:
                        datasource = json.load(fp)
                        datasource['created_at'] = parse_dt(datasource['created_at'].split('.')[0])
                        datasource['updated_at'] = parse_dt(datasource['updated_at'].split('.')[0])
                        datasource_arr.append(datasource)
                    except Exception as e:
                        print(e)
            except Exception as e:
                print(e)
        return datasource_arr

    def get_data(self, name, where=None, limit=None, offset=None):
        # @TODO Apply filter directly to postgres/mysql/clickhouse/etc...  when the datasource is of that type
        return get_sqlite_data(os.path.join(self.dir, name, 'sqlite.db'), where=where, limit=limit, offset=offset)

    def get_datasource(self, name):
        for ds in self.get_datasources():
            if ds['name'] == name:
                return ds
        return None

    def delete_datasource(self, name):
        shutil.rmtree(os.path.join(self.dir, name))

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
                print('Create URL data source !')
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

            df_with_types = cast_df_columns_types(df, self.get_analysis(df)['data_analysis_v2'])
            create_sqlite_db(os.path.join(ds_meta_dir, 'sqlite.db'), df_with_types)

            with open(os.path.join(ds_meta_dir, 'ds.pickle'), 'wb') as fp:
                pickle.dump(picklable, fp)

            with open(os.path.join(ds_meta_dir, 'metadata.json'), 'w') as fp:
                meta = {
                    'name': name,
                    'source_type': source_type,
                    'source': source,
                    'created_at': str(datetime.datetime.now()).split('.')[0],
                    'updated_at': str(datetime.datetime.now()).split('.')[0],
                    'row_count': len(df),
                    'columns': [dict(name=x) for x in list(df.keys())]
                }
                json.dump(meta, fp, indent=4, sort_keys=True)

            with open(os.path.join(ds_meta_dir, 'versions.json'), 'wt') as fp:
                json.dump(self.config.versions, fp, indent=4, sort_keys=True)

        except Exception:
            if os.path.isdir(ds_meta_dir):
                shutil.rmtree(ds_meta_dir)
            raise

        return self.get_datasource_obj(name, raw=True), name

    def get_datasource_obj(self, name, raw=False):
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
