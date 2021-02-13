import json
import datetime
import shutil
import os
import pickle

import pandas as pd

from mindsdb.interfaces.native.native import NativeInterface
from mindsdb_native import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.db import session, Datasource
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.log import log


class DataStore():
    def __init__(self):
        self.config = Config()

        self.fs_store = FsSotre()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.dir = self.config.paths['datasources']
        self.mindsdb_native = NativeInterface()

    def get_analysis(self, name):
        datasource_record = session.query(Datasource).filter_by(company_id=self.company_id, name=name).first()
        if datasource_record.analysis is None:
            datasource_record.analysis = json.dumps(self.mindsdb_native.analyse_dataset(self.get_datasource_obj(name)))
            session.commit()

        analysis = json.loads(datasource_record.analysis)
        return analysis

    def get_datasources(self, name=None):
        datasource_arr = []
        if name is not None:
            datasource_record_arr = session.query(Datasource).filter_by(company_id=self.company_id, name=name)
        else:
            datasource_record_arr = session.query(Datasource).filter_by(company_id=self.company_id)
        for datasource_record in datasource_record_arr:
            try:
                datasource = json.loads(datasource_record.data)
                datasource['created_at'] = datasource_record.created_at
                datasource['updated_at'] = datasource_record.updated_at
                datasource['name'] = datasource_record.name
                datasource['id'] = datasource_record.id
                datasource_arr.append(datasource)
            except Exception as e:
                log.error(e)
        return datasource_arr

    def get_data(self, name, where=None, limit=None, offset=None):
        offset = 0 if offset is None else offset
        ds = self.get_datasource_obj(name)

        if limit is not None:
            # @TODO Add `offset` to the `filter` method of the datasource and get rid of `offset`
            filtered_ds = ds.filter(where=where, limit=limit + offset).iloc[offset:]
        else:
            filtered_ds = ds.filter(where=where)

        filtered_ds = filtered_ds.where(pd.notnull(filtered_ds), None)
        data = filtered_ds.to_dict(orient='records')
        return {
            'data': data,
            'rowcount': len(ds),
            'columns_names': filtered_ds.columns
        }

    def get_datasource(self, name):
        datasource_arr = self.get_datasources(name)
        if len(datasource_arr) == 1:
            return datasource_arr[0]
        # @TODO: Remove when db swithc is more stable, this should never happen, but good santiy check while this is kinda buggy
        elif len(datasource_arr) > 1:
            log.error('Two or more datasource with the same name, (', len(datasource_arr), ') | Full list: ', datasource_arr)
            raise Exception('Two or more datasource with the same name')
        return None

    def delete_datasource(self, name):
        datasource_record = Datasource.query.filter_by(company_id=self.company_id, name=name).first()
        id = datasource_record.id
        session.delete(datasource_record)
        session.commit()
        self.fs_store.delete(f'datasource_{self.company_id}_{datasource_record.id}')
        try:
            shutil.rmtree(os.path.join(self.dir, name))
        except Exception:
            pass

    def save_datasource(self, name, source_type, source, file_path=None):
        datasource_record = Datasource(company_id=self.company_id, name=name)

        if source_type == 'file' and (file_path is None):
            raise Exception('`file_path` argument required when source_type == "file"')

        ds_meta_dir = os.path.join(self.dir, name)
        os.mkdir(ds_meta_dir)

        session.add(datasource_record)
        session.commit()
        datasource_record = session.query(Datasource).filter_by(company_id=self.company_id, name=name).first()

        try:
            if source_type == 'file':
                source = os.path.join(ds_meta_dir, source)
                shutil.move(file_path, source)
                ds = FileDS(source)

                creation_info = {
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
                    creation_info = {
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
                    ds = dsClass(**creation_info['kwargs'])

                elif integration['type'] in ['mssql', 'postgres', 'mariadb', 'mysql']:
                    creation_info = {
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
                        creation_info['kwargs']['database'] = integration['database']

                    if 'database' in source:
                        creation_info['kwargs']['database'] = source['database']

                    ds = dsClass(**creation_info['kwargs'])

                elif integration['type'] == 'snowflake':
                    creation_info = {
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

                    ds = dsClass(**creation_info['kwargs'])

                elif integration['type'] == 'mongodb':
                    if isinstance(source['find'], str):
                        source['find'] = json.loads(source['find'])
                    creation_info = {
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

                    ds = dsClass(**creation_info['kwargs'])
            else:
                # This probably only happens for urls
                ds = FileDS(source)
                creation_info = {
                    'class': 'FileDS',
                    'args': [source],
                    'kwargs': {}
                }

            df = ds.df

            if '' in df.columns or len(df.columns) != len(set(df.columns)):
                shutil.rmtree(ds_meta_dir)
                raise Exception('Each column in datasource must have unique non-empty name')

            datasource_record.creation_info = json.dumps(creation_info)
            datasource_record.data = json.dumps({
                'source_type': source_type,
                'source': source,
                'row_count': len(df),
                'columns': [dict(name=x) for x in list(df.keys())]
            })

            self.fs_store.put(name, f'datasource_{self.company_id}_{datasource_record.id}', self.dir)

        except Exception:
            if os.path.isdir(ds_meta_dir):
                shutil.rmtree(ds_meta_dir)
            raise

        session.commit()
        return self.get_datasource_obj(name, raw=True), name

    def get_datasource_obj(self, name, raw=False):
        try:
            datasource_record = session.query(Datasource).filter_by(company_id=self.company_id, name=name).first()
            self.fs_store.get(name, f'datasource_{self.company_id}_{datasource_record.id}', self.dir)
            creation_info = json.loads(datasource_record.creation_info)
            if raw:
                return creation_info
            else:
                return eval(creation_info['class'])(*creation_info['args'], **creation_info['kwargs'])
        except Exception as e:
            log.error(f'\n{e}\n')
            return None
