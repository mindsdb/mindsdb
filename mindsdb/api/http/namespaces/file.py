import os
import shutil
import tarfile
import tempfile
import zipfile
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
        """add new file
        params in FormData:
            - file
            - original_file_name [optional]
        """

        data = {}
        mindsdb_file_name = name.lower()

        existing_file_names = ca.file_controller.get_files_names()

        def on_field(field):
            name = field.field_name.decode()
            value = field.value.decode()
            data[name] = value

        file_object = None

        def on_file(file):
            nonlocal file_object
            data["file"] = file.file_name.decode()
            file_object = file.file_object

        temp_dir_path = tempfile.mkdtemp(prefix="mindsdb_file_")

        if request.headers["Content-Type"].startswith("multipart/form-data"):
            parser = multipart.create_form_parser(
                headers=request.headers,
                on_field=on_field,
                on_file=on_file,
                config={
                    "UPLOAD_DIR": temp_dir_path.encode(),  # bytes required
                    "UPLOAD_KEEP_FILENAME": True,
                    "UPLOAD_KEEP_EXTENSIONS": True,
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
                file_object = None
        else:
            data = request.json

        if mindsdb_file_name in existing_file_names:
            return http_error(
                400,
                "File already exists",
                f"File with name '{data['file']}' already exists",
            )

        if data.get("source_type") == "url":
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

            url_file_upload_enabled = config["url_file_upload"]["enabled"]
            if url_file_upload_enabled is False:
                return http_error(400, "URL file upload is disabled.", "URL file upload is disabled.")

            allowed_origins = config["url_file_upload"]["allowed_origins"]
            disallowed_origins = config["url_file_upload"]["disallowed_origins"]

            if validate_urls(url, allowed_origins, disallowed_origins) is False:
                return http_error(
                    400,
                    "Invalid URL",
                    "URL is not allowed for security reasons. Allowed hosts are: "
                    f"{', '.join(allowed_origins) if allowed_origins else 'not specified'}.",
                )

            data["file"] = clear_filename(data["name"])
            is_cloud = config.get("cloud", False)
            if is_cloud:
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
                            "Сan't determine remote file size",
                        )
                    if file_size > MAX_FILE_SIZE:
                        return http_error(400, "File is too big", f"Upload limit for file is {MAX_FILE_SIZE >> 20} MB")
            with requests.get(url, stream=True) as r:
                if r.status_code != 200:
                    return http_error(400, "Error getting file", f"Got status code: {r.status_code}")
                file_path = os.path.join(temp_dir_path, data["file"])
                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

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
        except Exception as e:
            logger.error(e)
            return http_error(
                400,
                "Error deleting file",
                f"There was an error while tring to delete file with name '{name}'",
            )
        return "", 200
