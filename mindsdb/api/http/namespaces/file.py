import os
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import multipart
import requests
from flask import current_app as ca
from flask import request
from flask_restx import Resource

from mindsdb.api.http.namespaces.configs.files import ns_conf
from mindsdb.api.http.utils import http_error
from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.utilities.config import config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log
from mindsdb.utilities.security import is_private_url, clear_filename, validate_urls
from mindsdb.utilities.fs import safe_extract
from mindsdb.integrations.utilities.files.file_reader import FileProcessingError

logger = log.getLogger(__name__)
MAX_FILE_SIZE = 1024 * 1024 * 100  # 100Mb


@ns_conf.route("/")
class FilesList(Resource):
    @ns_conf.doc("get_files_list")
    @api_endpoint_metrics("GET", "/files")
    def get(self):
        """List all files"""
        return ca.file_controller.get_files()


@ns_conf.route("/<name>")
@ns_conf.param("name", "MindsDB's name for file")
class File(Resource):
    @ns_conf.doc("put_file")
    @api_endpoint_metrics("PUT", "/files/file")
    def put(self, name: str):
        """add new file as table

        The table name is <name> path paramether

        Data is provided as json or form data. File can be provided with FormData or via URL.

        If file is in FormData, then the form contain:
            - source_type (str) - 'file'
            - file (binary) - the file itself
            - original_file_name (str, optional) - the name with which the file will be saved

        If file should be uploaded from URL:
            - source_type (str) - 'url'
            - source (str) - the URL
            - original_file_name (str, optional) - the name with which the file will be saved
        """

        data = {}
        mindsdb_file_name = name.lower()

        def on_field(field):
            name = field.field_name.decode()
            value = field.value.decode()
            data[name] = value

        file_object = None

        def on_file(file):
            nonlocal file_object
            file_name = file.file_name.decode()
            data["file"] = file_name
            if Path(file_name).name != file_name:
                raise ValueError(f"Wrong file name: {file_name}")
            file_object = file.file_object

        temp_dir_path = tempfile.mkdtemp(prefix="mindsdb_file_")

        if request.headers["Content-Type"].startswith("multipart/form-data"):
            parser = multipart.create_form_parser(
                headers=request.headers,
                on_field=on_field,
                on_file=on_file,
                config={
                    "UPLOAD_DIR": temp_dir_path.encode(),  # bytes required
                    "UPLOAD_KEEP_FILENAME": False,
                    "UPLOAD_KEEP_EXTENSIONS": True,
                    "UPLOAD_DELETE_TMP": False,
                    "MAX_MEMORY_FILE_SIZE": 0,
                },
            )

            while True:
                chunk = request.stream.read(8192)
                if not chunk:
                    break
                parser.write(chunk)
            parser.finalize()
            parser.close()

            if file_object is not None:
                if not file_object.closed:
                    try:
                        file_object.flush()
                    except (AttributeError, ValueError, OSError):
                        logger.debug("Failed to flush file_object before closing.", exc_info=True)
                    file_object.close()
                Path(file_object.name).rename(Path(file_object.name).parent / data["file"])
                file_object = None
        else:
            data = request.json

        existing_file_names = ca.file_controller.get_files_names(lower=True)
        if mindsdb_file_name.lower() in existing_file_names:
            return http_error(
                400,
                "File already exists",
                f"File with name '{mindsdb_file_name}' already exists",
            )

        source_type = data.get("source_type", "file")
        if source_type not in ("file", "url"):
            return http_error(
                400,
                "Wrong file source type",
                f'Only "file" and "url" supported as file source, got "{source_type}"',
            )

        if source_type == "url":
            url_file_upload_enabled = config["url_file_upload"]["enabled"]
            if url_file_upload_enabled is False:
                return http_error(400, "URL file upload is disabled.", "URL file upload is disabled.")

            if "file" in data:
                return http_error(
                    400,
                    "Fields conflict",
                    'URL source type can not be used together with "file" field.',
                )
            if "source" not in data:
                return http_error(
                    400,
                    "Missed file source",
                    'If the file\'s source type is URL, the "source" field should be specified.',
                )
            url = data["source"]
            try:
                url = urlparse(url)
                if not (url.scheme and url.netloc):
                    raise ValueError()
                url = url.geturl()
            except Exception:
                return http_error(
                    400,
                    "Invalid URL",
                    f"The URL is not valid: {data['source']}",
                )

            allowed_origins = config["url_file_upload"]["allowed_origins"]
            disallowed_origins = config["url_file_upload"]["disallowed_origins"]

            if validate_urls(url, allowed_origins, disallowed_origins) is False:
                return http_error(
                    400,
                    "Invalid URL",
                    "URL is not allowed for security reasons. Allowed hosts are: "
                    f"{', '.join(allowed_origins) if allowed_origins else 'not specified'}.",
                )

            data["file"] = clear_filename(mindsdb_file_name)
            if config.is_cloud:
                if is_private_url(url):
                    return http_error(400, f"URL is private: {url}")

                if ctx.user_class != 1:
                    info = requests.head(url, timeout=30)
                    file_size = info.headers.get("Content-Length")
                    try:
                        file_size = int(file_size)
                    except Exception:
                        pass

                    if file_size is None:
                        return http_error(
                            400,
                            "Error getting file info",
                            "Can't determine remote file size",
                        )
                    if file_size > MAX_FILE_SIZE:
                        return http_error(
                            400,
                            "File is too big",
                            f"Upload limit for file is {MAX_FILE_SIZE >> 20} MB",
                        )
            with requests.get(url, stream=True) as r:
                if r.status_code != 200:
                    return http_error(400, "Error getting file", f"Got status code: {r.status_code}")
                file_path = os.path.join(temp_dir_path, data["file"])
                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

        if "file" not in data:
            return http_error(
                400,
                "File field is missed",
                'The "field" field is missed in the form',
            )

        original_file_name = clear_filename(data.get("original_file_name"))

        file_path = os.path.join(temp_dir_path, data["file"])
        lp = file_path.lower()
        if lp.endswith((".zip", ".tar.gz")):
            if lp.endswith(".zip"):
                with zipfile.ZipFile(file_path) as f:
                    f.extractall(temp_dir_path)
            elif lp.endswith(".tar.gz"):
                with tarfile.open(file_path) as f:
                    safe_extract(f, temp_dir_path)
            os.remove(file_path)
            files = os.listdir(temp_dir_path)
            if len(files) != 1:
                os.rmdir(temp_dir_path)
                return http_error(400, "Wrong content.", "Archive must contain only one data file.")
            file_path = os.path.join(temp_dir_path, files[0])
            mindsdb_file_name = files[0]
            if not os.path.isfile(file_path):
                os.rmdir(temp_dir_path)
                return http_error(400, "Wrong content.", "Archive must contain data file in root.")

        try:
            if not Path(mindsdb_file_name).suffix == "":
                return http_error(400, "Error", "File name cannot contain extension.")
            ca.file_controller.save_file(mindsdb_file_name, file_path, file_name=original_file_name)
        except FileProcessingError as e:
            return http_error(400, "Error", str(e))
        except Exception as e:
            return http_error(500, "Error", str(e))
        finally:
            shutil.rmtree(temp_dir_path, ignore_errors=True)

        return "", 200

    @ns_conf.doc("delete_file")
    @api_endpoint_metrics("DELETE", "/files/file")
    def delete(self, name: str):
        """delete file"""

        try:
            ca.file_controller.delete_file(name)
        except FileNotFoundError:
            logger.exception(f"Error when deleting file '{name}'")
            return http_error(
                400,
                "Error deleting file",
                f"There was an error while trying to delete file with name '{name}'",
            )
        except Exception as e:
            logger.error(e)
            return http_error(
                500,
                "Error occured while deleting file",
                f"There was an error while trying to delete file with name '{name}'",
            )
        return "", 200
