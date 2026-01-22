import json
import os
import shutil
from pathlib import Path

import pandas as pd

from mindsdb.interfaces.storage import db
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from sqlalchemy.orm.attributes import flag_modified

from mindsdb.integrations.utilities.files.file_reader import FileReader


logger = log.getLogger(__name__)


class FileController:
    def __init__(self):
        self.config = Config()
        self.fs_store = FsStore()
        self.dir = os.path.join(self.config.paths["content"], "file")

    @staticmethod
    def _get_file_dir(file_id: int) -> str:
        """Get the file directory path with user namespace."""
        return f"file_{ctx.company_id}_{ctx.user_id}_{file_id}"

    @staticmethod
    def _get_legacy_file_dir(file_id: int) -> str:
        """Get the legacy file directory path (without user namespace)."""
        return f"file_{ctx.company_id}_{file_id}"

    def _migrate_legacy_file_path(self, file_record):
        """
        Migrate legacy file paths from file_{company_id}_{file_id} to file_{company_id}_{user_id}_{file_id}.
        Updates the file_record.file_path if migration occurs.

        Args:
            file_record: The file database record.

        Raises:
            Exception: If the file path migration fails.
        """
        old_path = self._get_legacy_file_dir(file_record.id)
        new_path = self._get_file_dir(file_record.id)

        # Skip if already migrated
        if file_record.file_path == new_path:
            return

        try:
            old_dir = Path(self.dir).joinpath(old_path)
            new_dir = Path(self.dir).joinpath(new_path)

            # Try to get from fs_store first (might be in remote storage)
            try:
                self.fs_store.get(old_path, base_dir=self.dir)
            except (FileNotFoundError, OSError) as e:
                logger.warning(f"Could not fetch legacy file {old_path} from remote: {e}")
            except Exception:
                logger.exception(f"Unexpected error fetching legacy file {old_path} from remote")
                raise

            if old_dir.exists() and not new_dir.exists():
                logger.info(f"Migrating legacy file path {old_path} to {new_path}")
                shutil.move(str(old_dir), str(new_dir))

                # Update remote storage
                self.fs_store.put(new_path, base_dir=self.dir)
                try:
                    self.fs_store.delete(old_path)
                except (FileNotFoundError, OSError) as e:
                    logger.warning(f"Could not delete legacy file {old_path} from remote: {e}")
                except Exception:
                    logger.exception(f"Unexpected error deleting legacy file {old_path} from remote")
                    raise

                # Update database record
                file_record.file_path = new_path
                db.session.commit()
        except Exception:
            logger.exception(f"Failed to migrate legacy file path {old_path} to {new_path}")
            raise

    def get_files_names(self, lower: bool = False):
        """return list of files names

        Args:
            lower (bool): return names in lowercase if True

        Returns:
            list[str]: list of files names
        """
        names = [
            record[0]
            for record in db.session.query(db.File.name).filter_by(company_id=ctx.company_id, user_id=ctx.user_id)
        ]
        if lower:
            names = [name.lower() for name in names]
        return names

    def get_file_meta(self, name):
        file_record = (
            db.session.query(db.File).filter_by(company_id=ctx.company_id, user_id=ctx.user_id, name=name).first()
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
        file_records = db.session.query(db.File).filter_by(company_id=ctx.company_id, user_id=ctx.user_id).all()
        files_metadata = [
            {
                "name": record.name,
                "row_count": record.row_count,
                "columns": record.columns,
            }
            for record in file_records
        ]
        return files_metadata

    def save_file(self, name, file_path, file_name=None):
        """Save the file to our store

        Args:
            name (str): with that name file will be available in sql api
            file_name (str): file name
            file_path (str): path to the file

        Returns:
            int: id of 'file' record in db
        """
        files_metadata = self.get_files()
        if name in [x["name"] for x in files_metadata]:
            raise Exception(f"File already exists: {name}")

        if file_name is None:
            file_name = Path(file_path).name

        file_dir = None
        try:
            pages_files, pages_index = self.get_file_pages(file_path)

            metadata = {"is_feather": True, "pages": pages_index}
            df = pages_files[0]
            file_record = db.File(
                name=name,
                company_id=ctx.company_id,
                user_id=ctx.user_id,
                source_file_path=file_name,
                file_path="",
                row_count=len(df),
                columns=list(df.columns),
                metadata_=metadata,
            )
            db.session.add(file_record)
            db.session.flush()

            store_file_path = self._get_file_dir(file_record.id)
            file_record.file_path = store_file_path

            file_dir = Path(self.dir).joinpath(store_file_path)
            file_dir.mkdir(parents=True, exist_ok=True)

            self.store_pages_as_feather(file_dir, pages_files)
            # store original file
            shutil.move(file_path, str(file_dir.joinpath(file_name)))

            self.fs_store.put(store_file_path, base_dir=self.dir)
            db.session.commit()

        except Exception:
            logger.exception("An error occurred while saving the file:")
            if file_dir is not None:
                shutil.rmtree(file_dir)
            raise

        return file_record.id

    def get_file_pages(self, source_path: str):
        """
        Reads file and extract pages from it
        Returned structures:
          - page_files: dict with content, {page_num: dataframe}
          - pages_index: dict, link between page name and num: {page_name: page_num}
        """
        file_reader = FileReader(path=source_path)
        tables = file_reader.get_contents()

        pages_files = {}
        pages_index = {}
        if len(tables) == 1:
            df = list(tables.values())[0]
            pages_files[0] = df
        else:
            # file has several pages, create a new one with info
            df = pd.DataFrame(tables.keys(), columns=["Tables"])
            pages_files[0] = df
            for i, page_name in enumerate(tables.keys(), 1):
                pages_files[i] = tables[page_name]
                pages_index[page_name] = i
        return pages_files, pages_index

    def store_pages_as_feather(self, dest_dir: Path, pages_files: dict):
        """
        Stores pages in file storage dir in feather format
        """

        for num, df in pages_files.items():
            dest = dest_dir.joinpath(f"{num}.feather")
            df.to_feather(str(dest))

    def delete_file(self, name):
        file_record = (
            db.session.query(db.File).filter_by(company_id=ctx.company_id, user_id=ctx.user_id, name=name).first()
        )
        if file_record is None:
            raise FileNotFoundError(f"File '{name}' does not exists")
        file_id = file_record.id

        # Try to delete both old and new path formats for backwards compatibility
        old_path = self._get_legacy_file_dir(file_id)
        new_path = self._get_file_dir(file_id)

        db.session.delete(file_record)
        db.session.commit()

        # Delete from storage (try both paths)
        for path in [new_path, old_path]:
            try:
                self.fs_store.delete(path)
            except (FileNotFoundError, OSError) as e:
                # Log if file doesn't exist, this is expected for one of the paths
                logger.warning(f"Failed to delete file {path}: {e}")
            except Exception:
                logger.exception(f"Unexpected error deleting file {path}")
                raise

        return True

    def get_file_path(self, name):
        file_record = (
            db.session.query(db.File).filter_by(company_id=ctx.company_id, user_id=ctx.user_id, name=name).first()
        )
        if file_record is None:
            raise FileNotFoundError(f"File '{name}' does not exists")

        # Migrate legacy file path if needed
        self._migrate_legacy_file_path(file_record)

        file_dir = self._get_file_dir(file_record.id)
        self.fs_store.get(file_dir, base_dir=self.dir)
        return str(Path(self.dir).joinpath(file_dir).joinpath(Path(file_record.source_file_path).name))

    def get_file_data(self, name: str, page_name: str = None) -> pd.DataFrame:
        """
        Returns file content as dataframe

        :param name: name of file
        :param page_name: page name, optional
        :return: Page or file content
        """
        file_record = (
            db.session.query(db.File).filter_by(company_id=ctx.company_id, user_id=ctx.user_id, name=name).first()
        )
        if file_record is None:
            raise FileNotFoundError(f"File '{name}' does not exists")

        # Migrate legacy file path if needed
        self._migrate_legacy_file_path(file_record)

        file_dir = self._get_file_dir(file_record.id)
        self.fs_store.get(file_dir, base_dir=self.dir)

        metadata = file_record.metadata_ or {}
        if metadata.get("is_feather") is not True:
            # migrate file

            file_path = Path(self.dir).joinpath(file_dir).joinpath(Path(file_record.source_file_path).name)

            pages_files, pages_index = self.get_file_pages(str(file_path))

            self.store_pages_as_feather(file_path.parent, pages_files)
            metadata["is_feather"] = True
            metadata["pages"] = pages_index

            file_record.metadata_ = metadata
            flag_modified(file_record, "metadata_")
            db.session.commit()

        if page_name is None:
            num = 0
        else:
            num = metadata.get("pages", {}).get(page_name)
            if num is None:
                raise KeyError(f"Page not found: {page_name}")

        path = Path(self.dir).joinpath(file_dir).joinpath(f"{num}.feather")
        return pd.read_feather(path)

    def set_file_data(self, name: str, df: pd.DataFrame, page_name: str = None):
        """
        Save file content
        :param name: name of file
        :param df: content to store
        :param page_name: name of page, optional
        """

        file_record = (
            db.session.query(db.File).filter_by(company_id=ctx.company_id, user_id=ctx.user_id, name=name).first()
        )
        if file_record is None:
            raise FileNotFoundError(f"File '{name}' does not exists")

        # Migrate legacy file path if needed
        self._migrate_legacy_file_path(file_record)

        file_dir = self._get_file_dir(file_record.id)
        self.fs_store.get(file_dir, base_dir=self.dir)

        num = 0
        if page_name is not None and file_record.metadata_ is not None:
            num = file_record.metadata_.get("pages", {}).get(page_name, 0)

        path = Path(self.dir).joinpath(file_dir).joinpath(f"{num}.feather")
        df.to_feather(path)
        self.fs_store.put(file_dir, base_dir=self.dir)
