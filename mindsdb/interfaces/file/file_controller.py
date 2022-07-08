import json
from pathlib import Path
import shutil

from mindsdb.interfaces.storage.db import session, Semaphor, Predictor, File
from mindsdb.integrations.handlers.file_handler import Handler as FileHandler
from mindsdb.utilities.log import log
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsStore


class FileController():
    def __init__(self):
        self.config = Config()
        self.fs_store = FsStore()
        self.dir = self.config['paths']['datasources']

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
            df, _col_map = FileHandler._handle_source(file_path)

            ds_meta = {
                'row_count': len(df),
                'column_names': list(df.columns)
            }

            file_record = File(
                name=name,
                company_id=company_id,
                source_file_path=file_name,
                file_path='',
                row_count=ds_meta['row_count'],
                columns=ds_meta['column_names']
            )
            session.add(file_record)
            session.commit()
            store_file_path = f'file_{company_id}_{file_record.id}'
            file_record.file_path = store_file_path
            session.commit()

            file_dir = Path(self.dir).joinpath(store_file_path)
            file_dir.mkdir(parents=True, exist_ok=True)
            source = file_dir.joinpath(file_name)
            # NOTE may be delay between db record exists and file is really in folder
            shutil.move(file_path, str(source))

            self.fs_store.put(store_file_path, store_file_path, self.dir)
        except Exception as e:
            log.error(e)
            raise
        finally:
            shutil.rmtree(file_dir)

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

    def get_file_path(self, name, company_id):
        file_record = session.query(File).filter_by(company_id=company_id, name=name).first()
        if file_record is None:
            raise Exception(f"File '{name}' does not exists")
        file_dir = f'file_{company_id}_{file_record.id}'
        self.fs_store.get(file_dir, file_dir, self.dir)
        return str(Path(self.dir).joinpath(file_dir).joinpath(Path(file_record.source_file_path).name))
