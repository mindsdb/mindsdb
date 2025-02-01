import traceback
import json
import csv
from io import BytesIO, StringIO
from pathlib import Path
import codecs

import filetype
import pandas as pd
from charset_normalizer import from_bytes
from langchain_text_splitters import RecursiveCharacterTextSplitter

from mindsdb.utilities import log

logger = log.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 500
DEFAULT_CHUNK_OVERLAP = 250


class FileDetectError(Exception):
    ...


def decode(file_obj: BytesIO) -> StringIO:
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

    def get(self, name, file_obj: BytesIO = None):
        format = self.get_format_by_name(name)
        if format is None and file_obj is not None:
            format = self.get_format_by_content(file_obj)

        if format is not None:
            return format
        raise FileDetectError(f'Unable to detect format: {name}')

    def get_format_by_name(self, filename):
        extension = Path(filename).suffix.strip(".").lower()
        if extension == "tsv":
            extension = "csv"
        return extension or None

    def get_format_by_content(self, file_obj):
        if self.is_parquet(file_obj):
            return "parquet"

        file_type = filetype.guess(file_obj)
        if file_type is None:
            return

        if file_type.mime in {
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
        }:
            return 'xlsx'

        if file_type.mime == 'application/pdf':
            return "pdf"

        file_obj = decode(file_obj)

        if self.is_json(file_obj):
            return "json"

        if self.is_csv(file_obj):
            return "csv"

    def is_json(self, data_obj: StringIO) -> bool:
        # see if its JSON
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
                finally:
                    data_obj.seek(0)
        return False

    def is_csv(self, data_obj: StringIO) -> bool:
        sample = data_obj.readline()  # trying to get dialect from header
        data_obj.seek(0)
        try:
            csv.Sniffer().sniff(sample)

        except Exception:
            return False

    def is_parquet(self, data: BytesIO) -> bool:
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


class FileReader:

    def _get_csv_dialect(self, buffer) -> csv.Dialect:
        sample = buffer.readline()  # trying to get dialect from header
        buffer.seek(0)
        try:
            if isinstance(sample, bytes):
                sample = sample.decode()
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

    def read(self, format, file_obj: BytesIO, **kwargs) -> pd.DataFrame:
        func = {
            'parquet': self.read_parquet,
            'csv': self.read_csv,
            'xlsx': self.read_excel,
            'pdf': self.read_pdf,
            'json': self.read_json,
            'txt': self.read_txt,
        }

        if format not in func:
            raise FileDetectError(f'Unsupported format: {format}')
        func = func[format]

        return func(file_obj, **kwargs)

    def read_csv(self, file_obj: BytesIO, **kwargs):
        file_obj = decode(file_obj)
        dialect = self._get_csv_dialect(file_obj)

        return pd.read_csv(file_obj, sep=dialect.delimiter, index_col=False)

    def read_txt(self, file_obj: BytesIO, **kwargs):
        file_obj = decode(file_obj)

        try:
            from langchain_core.documents import Document
        except ImportError:
            raise ImportError(
                "To import TXT document please install 'langchain-community':\n"
                "    pip install langchain-community"
            )
        text = file_obj.read()

        file_name = None
        if hasattr(file_obj, "name"):
            file_name = file_obj.name
        metadata = {"source": file_name}
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

    def read_pdf(self, file_obj: BytesIO, **kwargs):
        import fitz  # pymupdf

        with fitz.open(stream=file_obj) as pdf:  # open pdf
            text = chr(12).join([page.get_text() for page in pdf])

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=DEFAULT_CHUNK_SIZE, chunk_overlap=DEFAULT_CHUNK_OVERLAP
        )

        split_text = text_splitter.split_text(text)

        return pd.DataFrame(
            {"content": split_text, "metadata": [{}] * len(split_text)}
        )

    def read_json(self, file_obj: BytesIO, **kwargs):
        file_obj = decode(file_obj)
        file_obj.seek(0)
        json_doc = json.loads(file_obj.read())
        return pd.json_normalize(json_doc, max_level=0)

    def read_parquet(self, file_obj: BytesIO, **kwargs):
        return pd.read_parquet(file_obj)

    def read_excel(self, file_obj: BytesIO, sheet_name=None, **kwargs) -> pd.DataFrame:

        file_obj.seek(0)
        with pd.ExcelFile(file_obj) as xls:
            if sheet_name is None:
                # No sheet specified: Return list of sheets
                sheet_list = xls.sheet_names
                return pd.DataFrame(sheet_list, columns=["Sheet_Name"])
            else:
                # Specific sheet requested: Load that sheet
                return pd.read_excel(xls, sheet_name=sheet_name)
