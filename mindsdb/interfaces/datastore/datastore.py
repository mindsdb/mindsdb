import json
import shutil
import os
from pathlib import Path

import pandas as pd
from mindsdb_sql import parse_sql

import mindsdb_datasources
from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.interfaces.model.model_interface import ModelInterface
from mindsdb_datasources import (
    FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS,
    SnowflakeDS, AthenaDS, CassandraDS, ScyllaDS, TrinoDS
)
from mindsdb.utilities.config import Config
from mindsdb.utilities.log import log
from mindsdb.utilities.json_encoder import CustomJSONEncoder
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.interfaces.storage.db import session, Dataset, Semaphor, Predictor, Analysis, File
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.interfaces.database.views import ViewController


class QueryDS:
    def __init__(self, query, source, source_type, company_id):
        self.query = query
        self.source = source
        self.source_type = source_type
        self.company_id = company_id

    def query(self, q):
        pass

    @property
    def df(self):
        view_interface = WithKWArgsWrapper(
            ViewController(),
            company_id=self.company_id
        )

        integration_controller = WithKWArgsWrapper(
            IntegrationController(),
            company_id=self.company_id
        )

        data_store = WithKWArgsWrapper(
            DataStore(),
            company_id=self.company_id
        )

        query = self.query
        if self.source_type == 'view_query':
            if isinstance(query, str):
                query = parse_sql(query, dialect='mysql')

            table = query.from_table.parts[-1]
            view_metadata = view_interface.get(name=table)

            integration = integration_controller.get_by_id(view_metadata['integration_id'])
            integration_name = integration['name']

            dataset_name = data_store.get_vacant_name(table)
            data_store.save_datasource(dataset_name, integration_name, {'query': view_metadata['query']})
            try:
                dataset_object = data_store.get_datasource_obj(dataset_name)
                data_df = dataset_object.df
            finally:
                data_store.delete_datasource(dataset_name)
        else:
            raise Exception(f'Unknown source_type: {self.source_type}')
        return data_df


class DataStore():
    def __init__(self):
        self.config = Config()
        self.fs_store = FsStore()
        self.dir = self.config['paths']['datasources']
        self.model_interface = ModelInterface()

    def get_analysis(self, name, company_id=None):
        dataset_record = session.query(Dataset).filter_by(company_id=company_id, name=name).first()
        if dataset_record.analysis_id is None:
            return None
        analysis_record = session.query(Analysis).get(dataset_record.analysis_id)
        if analysis_record is None:
            return None
        analysis = json.loads(analysis_record.analysis)
        return analysis

    def start_analysis(self, name, company_id=None):
        dataset_record = session.query(Dataset).filter_by(company_id=company_id, name=name).first()
        if dataset_record.analysis_id is not None:
            return None

        semaphor_record = session.query(Semaphor).filter_by(
            company_id=company_id,
            entity_id=dataset_record.id,
            entity_type='dataset'
        ).first()

        if semaphor_record is None:
            semaphor_record = Semaphor(
                company_id=company_id,
                entity_id=dataset_record.id,
                entity_type='dataset',
                action='write'
            )
            session.add(semaphor_record)
            session.commit()
        else:
            return

        try:
            analysis = self.model_interface.analyse_dataset(
                ds=self.get_datasource_obj(name, raw=True, company_id=company_id),
                company_id=company_id
            )
            dataset_record = session.query(Dataset).filter_by(company_id=company_id, name=name).first()
            analysis_record = Analysis(analysis=json.dumps(analysis, cls=CustomJSONEncoder))
            session.add(analysis_record)
            session.flush()
            dataset_record.analysis_id = analysis_record.id
            session.commit()
        except Exception as e:
            log.error(e)
        finally:
            semaphor_record = session.query(Semaphor).filter_by(company_id=company_id, entity_id=dataset_record.id, entity_type='dataset').first()
            session.delete(semaphor_record)
            session.commit()

    def get_datasets(self, name=None, company_id=None):
        dataset_arr = []
        if name is not None:
            dataset_record_arr = session.query(Dataset).filter_by(company_id=company_id, name=name)
        else:
            dataset_record_arr = session.query(Dataset).filter_by(company_id=company_id)
        for dataset_record in dataset_record_arr:
            try:
                if dataset_record.data is None:
                    continue
                dataset = json.loads(dataset_record.data)
                dataset['created_at'] = dataset_record.created_at
                dataset['updated_at'] = dataset_record.updated_at
                dataset['name'] = dataset_record.name
                dataset['id'] = dataset_record.id
                dataset_arr.append(dataset)
            except Exception as e:
                log.error(e)
        return dataset_arr

    def get_files_names(self, company_id=None):
        """ return list of files names
        """
        return [x[0] for x in session.query(File.name).filter_by(company_id=company_id)]

    def get_file_meta(self, name, company_id=None):
        file_record = session.query(File).filter_by(company_id=company_id, name=name).first()
        if file_record is None:
            return None
        columns = file_record.columns
        if isinstance(columns, str):
            columns = json.loads(columns)
        return {
            'name': file_record.name,
            'columns': columns,
            'row_count': file_record.row_count
        }

    def get_data(self, name, where=None, limit=None, offset=None, company_id=None):
        offset = 0 if offset is None else offset
        ds = self.get_datasource_obj(name, company_id=company_id)

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
            'columns_names': list(data[0].keys())
        }

    def get_datasource(self, name, company_id=None):
        dataset_arr = self.get_datasets(name, company_id=company_id)
        if len(dataset_arr) == 1:
            return dataset_arr[0]
        # @TODO: Remove when db swithc is more stable, this should never happen, but good santiy check while this is kinda buggy
        elif len(dataset_arr) > 1:
            log.error('Two or more dataset with the same name, (', len(dataset_arr), ') | Full list: ', dataset_arr)
            raise Exception('Two or more dataset with the same name')
        return None

    def delete_datasource(self, name, company_id=None):
        dataset_record = Dataset.query.filter_by(company_id=company_id, name=name).first()
        if not Config()["force_dataset_removing"]:
            linked_models = Predictor.query.filter_by(company_id=company_id, dataset_id=dataset_record.id).all()
            if linked_models:
                raise Exception("Can't delete {} dataset because there are next models linked to it: {}".format(name, [model.name for model in linked_models]))
        session.query(Semaphor).filter_by(
            company_id=company_id, entity_id=dataset_record.id, entity_type='dataset'
        ).delete()
        session.delete(dataset_record)
        session.commit()
        self.fs_store.delete(f'datasource_{company_id}_{dataset_record.id}')  # TODO del in future
        try:
            shutil.rmtree(os.path.join(self.dir, f'{company_id}@@@@@{name}'))
        except Exception:
            pass

    def get_vacant_name(self, base=None, company_id=None):
        ''' returns name of dataset, which starts from 'base' and ds with that name is not exists yet
        '''
        if base is None:
            base = 'dataset'
        datasets = session.query(Dataset.name).filter_by(company_id=company_id).all()
        datasets_names = [x[0] for x in datasets]
        if base not in datasets_names:
            return base
        for i in range(1, 1000):
            candidate = f'{base}_{i}'
            if candidate not in datasets_names:
                return candidate
        raise Exception(f"Can not find appropriate name for dataset '{base}'")

    def create_datasource(self, source_type, source, file_path=None, company_id=None):
        integration_controller = IntegrationController()
        if source_type == 'view_query':
            dsClass = QueryDS
            creation_info = {
                'class': dsClass.__name__,
                'args': [],
                'kwargs': {
                    'query': source['query'],
                    'source': source['source'],   # view
                    'source_type': source_type,
                    'company_id': company_id
                }
            }

            ds = dsClass(**creation_info['kwargs'])
        elif source_type == 'file':
            file_name = source.get('mindsdb_file_name')
            file_record = session.query(File).filter_by(company_id=company_id, name=file_name).first()
            if file_record is None:
                raise Exception(f"Cant find file '{file_name}'")
            self.fs_store.get(f'{company_id}@@@@@{file_name}', f'file_{company_id}_{file_record.id}', self.dir)
            kwargs = {}
            query = source.get('query')
            if query is not None:
                kwargs['query'] = query

            path = Path(self.dir).joinpath(f'{company_id}@@@@@{file_name}').joinpath(file_record.source_file_path)

            creation_info = {
                'class': 'FileDS',
                'args': [str(path)],
                'kwargs': kwargs
            }
            ds = FileDS(str(path), **kwargs)

        elif integration_controller.get(source_type, company_id) is not None:
            integration = integration_controller.get(source_type, company_id)

            ds_class_map = {
                'clickhouse': ClickhouseDS,
                'mariadb': MariaDS,
                'mysql': MySqlDS,
                'singlestore': MySqlDS,
                'postgres': PostgresDS,
                'cockroachdb': PostgresDS,
                'mssql': MSSQLDS,
                'mongodb': MongoDS,
                'snowflake': SnowflakeDS,
                'athena': AthenaDS,
                'cassandra': CassandraDS,
                'scylladb': ScyllaDS,
                'trinodb': TrinoDS,
                'questdb': PostgresDS
            }

            try:
                dsClass = ds_class_map[integration['type']]
            except KeyError:
                raise KeyError(f"Unknown DS type: {source_type}, type is {integration['type']}")

            if dsClass is None:
                raise Exception(f"Unsupported dataset: {source_type}, type is {integration['type']}, please install required dependencies!")

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

            elif integration['type'] in ['mssql', 'postgres', 'cockroachdb', 'mariadb', 'mysql', 'singlestore', 'cassandra', 'scylladb', 'questdb']:
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
                kwargs = creation_info['kwargs']

                integration_folder_name = f'integration_files_{company_id}_{integration["id"]}'
                if integration['type'] in ('mysql', 'mariadb'):
                    kwargs['ssl'] = integration.get('ssl')
                    kwargs['ssl_ca'] = integration.get('ssl_ca')
                    kwargs['ssl_cert'] = integration.get('ssl_cert')
                    kwargs['ssl_key'] = integration.get('ssl_key')
                    for key in ['ssl_ca', 'ssl_cert', 'ssl_key']:
                        if isinstance(kwargs[key], str) and len(kwargs[key]) > 0:
                            kwargs[key] = os.path.join(
                                self.integrations_dir,
                                integration_folder_name,
                                kwargs[key]
                            )
                elif integration['type'] in ('cassandra', 'scylla'):
                    kwargs['secure_connect_bundle'] = integration.get('secure_connect_bundle')
                    if (
                        isinstance(kwargs['secure_connect_bundle'], str)
                        and len(kwargs['secure_connect_bundle']) > 0
                    ):
                        kwargs['secure_connect_bundle'] = os.path.join(
                            self.integrations_dir,
                            integration_folder_name,
                            kwargs['secure_connect_bundle']
                        )

                if 'database' in integration:
                    kwargs['database'] = integration['database']

                if 'database' in source:
                    kwargs['database'] = source['database']

                ds = dsClass(**kwargs)

            elif integration['type'] == 'snowflake':
                creation_info = {
                    'class': dsClass.__name__,
                    'args': [],
                    'kwargs': {
                        'query': source['query'],
                        'schema': source.get('schema', integration['schema']),
                        'warehouse': source.get('warehouse', integration['warehouse']),
                        'database': source.get('database', integration['database']),
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

            elif integration['type'] == 'athena':
                creation_info = {
                    'class': dsClass.__name__,
                    'args': [],
                    'kwargs': {
                        'query': source['query'],
                        'staging_dir': source['staging_dir'],
                        'database': source['database'],
                        'access_key': source['access_key'],
                        'secret_key': source['secret_key'],
                        'region_name': source['region_name']
                    }
                }

                ds = dsClass(**creation_info['kwargs'])

            elif integration['type'] == 'trinodb':
                creation_info = {
                    'class': dsClass.__name__,
                    'args': [],
                    'kwargs': {
                        'query': source['query'],
                        'user': integration['user'],
                        'password': integration['password'],
                        'host': integration['host'],
                        'port': integration['port'],
                        'schema': integration['schema'],
                        'catalog': integration['catalog']
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
        return ds, creation_info

    def get_files(self, company_id=None):
        """ Get list of files

            Returns:
                list[dict]: files metadata
        """
        file_records = session.query(File).filter_by(company_id=company_id).all()
        files_metadata = [{
            'name': record.name,
            'row_count': record.row_count,
            'columns': record.columns,
        } for record in file_records]
        return files_metadata

    def save_file(self, name, file_path, file_name=None, company_id=None):
        """ Save the file to our store

            Args:
                name (str): with that name file will be available in sql api
                file_name (str): file name
                file_path (str): path to the file
                company_id (int): company id

            Returns:
                int: id of 'file' record in db
        """
        files_metadata = self.get_files()
        if name in [x['name'] for x in files_metadata]:
            raise Exception(f'File already exists: {name}')

        if file_name is None:
            file_name = Path(file_path).name

        try:
            ds_meta_dir = Path(self.dir).joinpath(f'{company_id}@@@@@{name}')
            ds_meta_dir.mkdir()

            source = ds_meta_dir.joinpath(file_name)
            shutil.move(file_path, str(source))

            ds = FileDS(str(source))
            ds_meta = self._get_ds_meta(ds)

            column_names = ds_meta['column_names']
            if ds_meta['column_names'] is not None:
                column_names = json.dumps([dict(name=x) for x in ds_meta['column_names']])
            file_record = File(
                name=name,
                company_id=company_id,
                source_file_path=file_name,
                file_path=str(source),
                row_count=ds_meta['row_count'],
                columns=column_names
            )
            session.add(file_record)
            session.commit()
            self.fs_store.put(f'{company_id}@@@@@{name}', f'file_{company_id}_{file_record.id}', self.dir)
        except Exception as e:
            log.error(e)
            raise
        finally:
            shutil.rmtree(ds_meta_dir)

        return file_record.id

    def delete_file(self, name, company_id):
        file_record = session.query(File).filter_by(company_id=company_id, name=name).first()
        if file_record is None:
            return None
        file_id = file_record.id
        session.delete(file_record)
        session.commit()
        self.fs_store.delete(f'file_{company_id}_{file_id}')
        return True

    def _get_ds_meta(self, ds):
        if hasattr(ds, 'get_columns') and hasattr(ds, 'get_row_count'):
            try:
                column_names = ds.get_columns()
                row_count = ds.get_row_count()
            except Exception:
                df = ds.df
                column_names = list(df.keys())
                row_count = len(df)
        else:
            df = ds.df
            column_names = list(df.keys())
            row_count = len(df)
        return {
            'column_names': column_names,
            'row_count': row_count
        }

    def save_datasource(self, name, source_type, source=None, file_path=None, company_id=None):
        dataset_record = session.query(Dataset).filter_by(company_id=company_id, name=name).first()
        while dataset_record is not None:
            raise Exception(f'Dataset with name {name} already exists')

        if source_type == 'views':
            source_type = 'view_query'
        elif source_type == 'files':
            source_type = 'file'

        try:
            dataset_record = Dataset(
                company_id=company_id,
                name=name,
                datasources_version=mindsdb_datasources.__version__,
                mindsdb_version=mindsdb_version
            )
            session.add(dataset_record)
            session.commit()

            ds, creation_info = self.create_datasource(source_type, source, file_path, company_id)

            ds_meta = self._get_ds_meta(ds)
            column_names = ds_meta['column_names']
            row_count = ds_meta['row_count']

            dataset_record.ds_class = creation_info['class']
            dataset_record.creation_info = json.dumps(creation_info)
            dataset_record.data = json.dumps({
                'source_type': source_type,
                'source': source,
                'row_count': row_count,
                'columns': [dict(name=x) for x in column_names]
            })

            session.commit()

        except Exception as e:
            log.error(f'Error creating dataset {name}, exception: {e}')
            try:
                self.delete_datasource(name, company_id=company_id)
            except Exception:
                pass
            raise e

        return self.get_datasource_obj(name, raw=True, company_id=company_id)

    def get_datasource_obj(self, name=None, id=None, raw=False, company_id=None):
        try:
            if name is not None:
                dataset_record = session.query(Dataset).filter_by(company_id=company_id, name=name).first()
            else:
                dataset_record = session.query(Dataset).filter_by(company_id=company_id, id=id).first()

            creation_info = json.loads(dataset_record.creation_info)
            if raw:
                return creation_info
            else:
                if dataset_record.ds_class == 'FileDS':
                    file_record = session.query(File).filter_by(company_id=company_id, name=name).first()
                    if file_record is not None:
                        self.fs_store.get(f'{company_id}@@@@@{dataset_record.name}', f'file_{company_id}_{file_record.id}', self.dir)
                return eval(creation_info['class'])(*creation_info['args'], **creation_info['kwargs'])
        except Exception as e:
            log.error(f'Error getting dataset {name}, exception: {e}')
            return None

    def get_file_path(self, name, company_id):
        file_record = session.query(File).filter_by(company_id=company_id, name=name).first()
        if file_record is None:
            raise Exception("File '{name}' does not exists")
        file_dir = f'file_{company_id}_{file_record.id}'
        self.fs_store.get(file_dir, file_dir, self.dir)
        return str(Path(self.dir).joinpath(file_dir).joinpath(Path(file_record.source_file_path).name))
