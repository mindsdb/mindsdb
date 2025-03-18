import traceback
import json
import csv
from io import BytesIO, StringIO, IOBase
from pathlib import Path
import codecs
from typing import List

import filetype
import pandas as pd
from charset_normalizer import from_bytes
from langchain_text_splitters import RecursiveCharacterTextSplitter
import fitz  # pymupdf

from mindsdb.utilities import log

logger = log.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 500
DEFAULT_CHUNK_OVERLAP = 250


class FileDetectError(Exception):
    ...


def decode(file_obj: IOBase) -> StringIO:
    file_obj.seek(0)
    byte_str = file_obj.read()
    # Move it to StringIO
    try:
        # Handle Microsoft's BOM "special" UTF-8 encoding
        if byte_str.startswith(codecs.BOM_UTF8):
            data_str = StringIO(byte_str.decode("utf-8-sig"))
        else:
            file_encoding_meta = from_bytes(
                byte_str[: 32 * 1024],
                steps=32,  # Number of steps/block to extract from my_byte_str
                chunk_size=1024,  # Set block size of each extraction)
                explain=False,
            )
            best_meta = file_encoding_meta.best()
            errors = "strict"
            if best_meta is not None:
                encoding = file_encoding_meta.best().encoding

                try:
                    data_str = StringIO(byte_str.decode(encoding, errors))
                except UnicodeDecodeError:
                    encoding = "utf-8"
                    errors = "replace"

                    data_str = StringIO(byte_str.decode(encoding, errors))
            else:
                encoding = "utf-8"
                errors = "replace"

                data_str = StringIO(byte_str.decode(encoding, errors))
    except Exception as e:
        logger.error(traceback.format_exc())
        raise FileDetectError("Could not load into string") from e

    return data_str


class FormatDetector:

    supported_formats = ['parquet', 'csv', 'xlsx', 'pdf', 'json', 'txt']
    multipage_formats = ['xlsx']

    def __init__(
        self,
        path: str = None,
        name: str = None,
        file: IOBase = None
    ):
        """
        File format detector
        One of these arguments has to be passed: `path` or `file`

        :param path: path to the file
        :param name: name of the file
        :param file: file descriptor (via open(...), of BytesIO(...))
        """
        if path is not None:
            file = open(path, 'rb')

        elif file is not None:
            if name is None:
                if hasattr(file, 'name'):
                    path = file.name
                else:
                    path = 'file'
        else:
            raise FileDetectError('Wrong arguments: path or file is required')

        if name is None:
            name = Path(path).name

        self.name = name
        self.file_obj = file
        self.format = None

        self.parameters = {}

    def get_format(self) -> str:
        if self.format is not None:
            return self.format

        format = self.get_format_by_name()
        if format is not None:
            if format not in self.supported_formats:
                raise FileDetectError(f'Not supported format: {format}')

        if format is None and self.file_obj is not None:
            format = self.get_format_by_content()
            self.file_obj.seek(0)

        if format is None:
            raise FileDetectError(f'Unable to detect format: {self.name}')

        self.format = format
        return format

    def get_format_by_name(self):
        extension = Path(self.name).suffix.strip(".").lower()
        if extension == "tsv":
            extension = "csv"
            self.parameters['delimiter'] = '\t'

        return extension or None

    def get_format_by_content(self):
        if self.is_parquet(self.file_obj):
            return "parquet"

        file_type = filetype.guess(self.file_obj)
        if file_type is not None:

            if file_type.mime in {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/vnd.ms-excel",
            }:
                return 'xlsx'

            if file_type.mime == 'application/pdf':
                return "pdf"

        file_obj = decode(self.file_obj)

        if self.is_json(file_obj):
            return "json"

        if self.is_csv(file_obj):
            return "csv"

    @staticmethod
    def is_json(data_obj: StringIO) -> bool:
        # see if its JSON
        data_obj.seek(0)
        text = data_obj.read(100).strip()
        data_obj.seek(0)
        if len(text) > 0:
            # it looks like a json, then try to parse it
            if text.startswith("{") or text.startswith("["):
                try:
                    json.loads(data_obj.read())
                    return True
                except Exception:
                    return False
        return False

    @classmethod
    def is_csv(cls, data_obj: StringIO) -> bool:
        data_obj.seek(0)
        sample = data_obj.readline()  # trying to get dialect from header
        try:
            data_obj.seek(0)
            csv.Sniffer().sniff(sample)

            # Avoid a false-positive for json files
            if cls.is_json(data_obj):
                return False
            return True
        except Exception:
            return False

    @staticmethod
    def is_parquet(data: IOBase) -> bool:
        # Check first and last 4 bytes equal to PAR1.
        # Refer: https://parquet.apache.org/docs/file-format/
        parquet_sig = b"PAR1"
        data.seek(0, 0)
        start_meta = data.read(4)
        data.seek(-4, 2)
        end_meta = data.read()
        data.seek(0)
        if start_meta == parquet_sig and end_meta == parquet_sig:
            return True
        return False


class FileReader(FormatDetector):

    def _get_fnc(self):
        format = self.get_format()
        func = getattr(self, f'read_{format}', None)
        if func is None:
            raise FileDetectError(f'Unsupported format: {format}')
        return func

    def get_pages(self, **kwargs) -> List[str]:
        """
            Get list of tables in file
        """
        format = self.get_format()
        if format not in self.multipage_formats:
            # only one table
            return ['main']

        func = self._get_fnc()
        self.file_obj.seek(0)

        return [
            name for name, _ in
            func(self.file_obj, only_names=True, **kwargs)
        ]

    def get_contents(self, **kwargs):
        """
            Get all info(pages with content) from file as dict: {tablename, content}
        """
        func = self._get_fnc()
        self.file_obj.seek(0)

        format = self.get_format()
        if format not in self.multipage_formats:
            # only one table
            return {'main': func(self.file_obj, name=self.name, **kwargs)}

        return {
            name: df
            for name, df in
            func(self.file_obj, **kwargs)
        }

    def get_page_content(self, page_name: str = None, **kwargs) -> pd.DataFrame:
        """
            Get content of a single table
        """
        func = self._get_fnc()
        self.file_obj.seek(0)

        format = self.get_format()
        if format not in self.multipage_formats:
            # only one table
            return func(self.file_obj, name=self.name, **kwargs)

        for _, df in func(self.file_obj, name=self.name, page_name=page_name, **kwargs):
            return df

    @staticmethod
    def _get_csv_dialect(buffer, delimiter=None) -> csv.Dialect:
        sample = buffer.readline()  # trying to get dialect from header
        buffer.seek(0)
        try:
            if isinstance(sample, bytes):
                sample = sample.decode()

            if delimiter is not None:
                accepted_csv_delimiters = [delimiter]
            else:
                accepted_csv_delimiters = [",", "\t", ";"]
            try:
                dialect = csv.Sniffer().sniff(
                    sample, delimiters=accepted_csv_delimiters
                )
                dialect.doublequote = (
                    True  # assume that all csvs have " as string escape
                )
            except Exception:
                dialect = csv.reader(sample).dialect
                if dialect.delimiter not in accepted_csv_delimiters:
                    raise Exception(
                        f"CSV delimeter '{dialect.delimiter}' is not supported"
                    )

        except csv.Error:
            dialect = None
        return dialect

    @classmethod
    def read_csv(cls, file_obj: BytesIO, delimiter=None, **kwargs):
        file_obj = decode(file_obj)
        dialect = cls._get_csv_dialect(file_obj, delimiter=delimiter)

        return pd.read_csv(file_obj, sep=dialect.delimiter, index_col=False)

    @staticmethod
    def read_txt(file_obj: BytesIO, name=None, **kwargs):
        file_obj = decode(file_obj)

        try:
            from langchain_core.documents import Document
        except ImportError:
            raise ImportError(
                "To import TXT document please install 'langchain-community':\n"
                "    pip install langchain-community"
            )
        text = file_obj.read()

        metadata = {"source_file": name, "file_format": "txt"}
        documents = [Document(page_content=text, metadata=metadata)]

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=DEFAULT_CHUNK_SIZE, chunk_overlap=DEFAULT_CHUNK_OVERLAP
        )

        docs = text_splitter.split_documents(documents)
        return pd.DataFrame(
            [
                {"content": doc.page_content, "metadata": doc.metadata}
                for doc in docs
            ]
        )

    @staticmethod
    def read_pdf(file_obj: BytesIO, name=None, **kwargs):

        with fitz.open(stream=file_obj.read()) as pdf:  # open pdf
            text = chr(12).join([page.get_text() for page in pdf])

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=DEFAULT_CHUNK_SIZE, chunk_overlap=DEFAULT_CHUNK_OVERLAP
        )

        split_text = text_splitter.split_text(text)

        return pd.DataFrame(
            {"content": split_text, "metadata": [{"file_format": "pdf", "source_file": name}] * len(split_text)}
        )

    @staticmethod
    def read_json(file_obj: BytesIO, **kwargs):
        file_obj = decode(file_obj)
        file_obj.seek(0)
        json_doc = json.loads(file_obj.read())
        return pd.json_normalize(json_doc, max_level=0)

    @staticmethod
    def read_parquet(file_obj: BytesIO, **kwargs):
        return pd.read_parquet(file_obj)

    @staticmethod
    def read_xlsx(file_obj: BytesIO, page_name=None, only_names=False, **kwargs):
        with pd.ExcelFile(file_obj) as xls:

            if page_name is not None:
                # return specific page
                yield page_name, pd.read_excel(xls, sheet_name=page_name)

            for page_name in xls.sheet_names:

                if only_names:
                    # extract only pages names
                    df = None
                else:
                    df = pd.read_excel(xls, sheet_name=page_name)
                yield page_name, df
