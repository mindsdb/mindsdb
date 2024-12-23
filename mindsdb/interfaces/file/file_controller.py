import json
import os
import shutil
from pathlib import Path

from mindsdb.integrations.handlers.file_handler import Handler as FileHandler
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx

logger = log.getLogger(__name__)


class FileController:
    def __init__(self):
        self.config = Config()
        self.fs_store = FsStore()
        self.dir = os.path.join(self.config.paths["content"], "file")

    def get_files_names(self):
        """return list of files names"""
        return [
            x[0]
            for x in db.session.query(db.File.name).filter_by(company_id=ctx.company_id)
        ]

    def get_file_meta(self, name):
        file_record = (
            db.session.query(db.File)
            .filter_by(company_id=ctx.company_id, name=name)
            .first()
        )
        if file_record is None:
            return None
        columns = file_record.columns
        if isinstance(columns, str):
            columns = json.loads(columns)
        return {
            "name": file_record.name,
            "columns": columns,
            "row_count": file_record.row_count,
        }

    def get_files(self):
        """Get list of files

        Returns:
            list[dict]: files metadata
        """
        file_records = (
            db.session.query(db.File).filter_by(company_id=ctx.company_id).all()
        )
        files_metadata = [
            {
                "name": record.name,
                "row_count": record.row_count,
                "columns": record.columns,
            }
            for record in file_records
        ]
        return files_metadata
    
    def save_excel_file_with_sheets(self, name: str, file_path: str, sheet_names: str, file_name: str = None) -> list[int]:
        """
        Saves an uploaded Excel file with multiple sheets to the database, file system, file store, and returns the file record ID.

        Args:
            name (str): The name given to the file. This will be used to reference the file in the SQL API.
            file_path (str): The temporary path to the uploaded file.
            sheet_names (list): The names of the sheets in the Excel file to save.
            file_name (str): The name of the uploaded file.

        Returns:
            list[int]: The IDs of the file records in the database.
        """
        files_metadata = self.get_files()
        for sheet_name in sheet_names:
            name_for_sheet = f"{name}_{sheet_name}"

            if name_for_sheet in [x["name"] for x in files_metadata]:
                raise Exception(f"File already exists: {name_for_sheet}")

            if file_name is None:
                file_name_for_sheet = Path(file_path).name.split(".")[0] + f"_{sheet_name}.xlsx"
            
            file_record_ids = []
            file_record, storage_directory_name, stored_file_path, file_df = self._save_file(
                name_for_sheet,
                file_path,
                file_name=file_name_for_sheet,
                sheet_name=sheet_name
            )
            
            try:
                file_df.to_excel(str(stored_file_path), index=False)

                self.fs_store.put(storage_directory_name, base_dir=self.dir)
                file_record_ids.append(file_record.id)
            except Exception as e:
                logger.error(e)
                raise

        os.remove(file_path)
        return file_record_ids

    def save_file(self, name: str, file_path: str, file_name: str = None) -> int:
        """
        Saves an uploaded file to the database, file system, file store, and returns the file record ID.

        Args:
            name (str): The name given to the file. This will be used to reference the file in the SQL API.
            file_path (str): The temporary path to the uploaded file.
            file_name (str): The name of the uploaded file.

        Returns:
            int: The ID of the file record in the database.
        """
        files_metadata = self.get_files()
        if name in [x["name"] for x in files_metadata]:
            raise Exception(f"File already exists: {name}")

        if file_name is None:
            file_name = Path(file_path).name

        file_record, storage_directory_name, stored_file_path, _ = self._save_file(name, file_path, file_name=file_name)
        try:
            # NOTE: There may be a delay between file being saved and record being committed to the database.
            shutil.move(file_path, str(stored_file_path))
            self.fs_store.put(storage_directory_name, base_dir=self.dir)
        except Exception as e:
            logger.error(e)
            raise

        return file_record.id
    
    def _save_file(self, name: str, file_path: str, file_name: str, sheet_name: str = None):
        """
        Records the file in the database and creates a directory for it in the file system.

        Args:
            name (str): The name given to the file. This will be used to reference the file in the SQL API.
            file_path (str): The temporary path to the uploaded file.
            file_name (str): The name of the uploaded file.
            sheet_name (str): The name of the sheet in the Excel file to save. If None, the first sheet will be saved.

        Returns:
            tuple: The 'file' record in the database, the directory name of the stored file, the full path to the stored file, and the file contents as a DataFrame.
        """
        try:
            file_df, _ = FileHandler._handle_source(file_path, sheet_name=sheet_name)

            ds_meta = {"row_count": len(file_df), "column_names": list(file_df.columns)}

            file_record = db.File(
                name=name,
                company_id=ctx.company_id,
                source_file_path=file_name,
                file_path="",
                row_count=ds_meta["row_count"],
                columns=ds_meta["column_names"],
            )
            db.session.add(file_record)
            db.session.commit()

            # Create a directory path for storing the file, based on company ID and file record ID.
            storage_directory_name = f"file_{ctx.company_id}_{file_record.id}"
            file_record.file_path = storage_directory_name
            db.session.commit()
            
            # Create the full path to the storage directory.
            full_storage_directory_path = Path(self.dir).joinpath(storage_directory_name)
            # Create the storage directory if it does not exist.
            full_storage_directory_path.mkdir(parents=True, exist_ok=True)
            
            # Create the full path to the stored file.
            stored_file_path = full_storage_directory_path.joinpath(file_name)
            
            return file_record, storage_directory_name, stored_file_path, file_df
        except Exception as e:
            logger.error(e)
            if full_storage_directory_path.exists():
                shutil.rmtree(full_storage_directory_path)
            raise

    def delete_file(self, name):
        file_record = (
            db.session.query(db.File)
            .filter_by(company_id=ctx.company_id, name=name)
            .first()
        )
        if file_record is None:
            return None
        file_id = file_record.id
        db.session.delete(file_record)
        db.session.commit()
        self.fs_store.delete(f"file_{ctx.company_id}_{file_id}")
        return True

    def get_file_path(self, name):
        file_record = (
            db.session.query(db.File)
            .filter_by(company_id=ctx.company_id, name=name)
            .first()
        )
        if file_record is None:
            raise Exception(f"File '{name}' does not exists")
        file_dir = f"file_{ctx.company_id}_{file_record.id}"
        self.fs_store.get(file_dir, base_dir=self.dir)
        return str(
            Path(self.dir)
            .joinpath(file_dir)
            .joinpath(Path(file_record.source_file_path).name)
        )
