import os
import sys
import json
import argparse
import datetime
from pathlib import Path
from copy import deepcopy
import multiprocessing as mp

from appdirs import user_data_dir

# NOTE do not `import from mindsdb` here


def _merge_key_recursive(target_dict, source_dict, key):
    if key not in target_dict:
        target_dict[key] = source_dict[key]
    elif not isinstance(target_dict[key], dict) or not isinstance(source_dict[key], dict):
        target_dict[key] = source_dict[key]
    else:
        for k in list(source_dict[key].keys()):
            _merge_key_recursive(target_dict[key], source_dict[key], k)


def _merge_configs(original_config: dict, override_config: dict) -> dict:
    for key in list(override_config.keys()):
        _merge_key_recursive(original_config, override_config, key)
    return original_config


def _overwrite_configs(original_config: dict, override_config: dict) -> dict:
    """Overwrite original config with override config."""
    for key in list(override_config.keys()):
        original_config[key] = override_config[key]
    return original_config


def create_data_dir(path: Path) -> None:
    """Create a directory and checks that it is writable.

    Args:
        path (Path): path to create and check

    Raises:
        NotADirectoryError: if path exists, but it is not a directory
        PermissionError: if path exists/created, but it is not writable
        Exception: if directory could not be created
    """
    if path.exists() and not path.is_dir():
        raise NotADirectoryError(f"The path is not a directory: {path}")

    try:
        path.mkdir(mode=0o777, exist_ok=True, parents=True)
    except Exception as e:
        raise Exception("MindsDB storage directory could not be created") from e

    if not os.access(path, os.W_OK):
        raise PermissionError(f"The directory is not allowed for writing: {path}")


class Config:
    """Application config. Singletone, initialized just once. Re-initialyze if `config.auto.json` is changed.
    The class loads multiple configs and merge then in one. If a config option defined in multiple places (config file,
    env var, cmd arg, etc), then it will be resolved in following order of priority:
     - default config values (lowest priority)
     - `config.json` provided by the user
     - `config.auto.json`
     - config values collected from env vars
     - values from cmd args (most priority)

    Attributes:
        __instance (Config): instance of 'Config' to make it singleton
        _config (dict): application config, the result of merging other configs
        _user_config (dict): config provided by the user (usually with cmd arg `--config=config.json`)
        _env_config (dict): config collected from different env vars
        _auto_config (dict): config that is editd by the app itself (e.g. when you change values in GUI)
        _default_config (dict): config with default values
        config_path (Path): path to the `config.json` provided by the user
        storage_root_path (Path): path to storage root folder
        auto_config_path (Path): path to `config.auto.json`
        auto_config_mtime (float): mtime of `config.auto.json` when it was loaded to `self._auto_config`
        _cmd_args (argparse.Namespace): cmd args
        use_docker_env (bool): is the app run in docker env
    """

    __instance: "Config" = None

    _config: dict = None
    _user_config: dict = None
    _env_config: dict = None
    _auto_config: dict = None
    _default_config: dict = None
    config_path: Path = None
    storage_root_path: Path = None
    auto_config_path: Path = None
    auto_config_mtime: float = 0
    _cmd_args: argparse.Namespace = None
    use_docker_env: bool = os.environ.get("MINDSDB_DOCKER_ENV", False) is not False

    def __new__(cls, *args, **kwargs) -> "Config":
        """Make class singletone and initialize config."""
        if cls.__instance is not None:
            return cls.__instance

        self = super().__new__(cls, *args, **kwargs)
        cls.__instance = self

        self.fetch_user_config()

        # region determine root path
        if self.storage_root_path is None:
            if isinstance(os.environ.get("MINDSDB_STORAGE_DIR"), str):
                self.storage_root_path = os.environ["MINDSDB_STORAGE_DIR"]
            elif "root" in self._user_config.get("paths", {}):
                self.storage_root_path = self.user_config["paths"]["root"]
            else:
                self.storage_root_path = os.path.join(user_data_dir("mindsdb", "mindsdb"), "var/")
            self.storage_root_path = Path(self.storage_root_path)
            create_data_dir(self.storage_root_path)
        # endregion

        # region prepare default config
        api_host = "127.0.0.1" if not self.use_docker_env else "0.0.0.0"
        self._default_config = {
            "permanent_storage": {"location": "absent"},
            "storage_db": (
                "sqlite:///"
                + str(self.storage_root_path / "mindsdb.sqlite3.db")
                + "?check_same_thread=False&timeout=30"
            ),
            "paths": {
                "root": self.storage_root_path,
                "content": self.storage_root_path / "content",
                "storage": self.storage_root_path / "storage",
                "static": self.storage_root_path / "static",
                "tmp": self.storage_root_path / "tmp",
                "log": self.storage_root_path / "log",
                "cache": self.storage_root_path / "cache",
                "locks": self.storage_root_path / "locks",
            },
            "auth": {
                "http_auth_enabled": False,
                "http_permanent_session_lifetime": datetime.timedelta(days=31),
                "username": "mindsdb",
                "password": "",
            },
            "logging": {
                "handlers": {
                    "console": {
                        "enabled": True,
                        "formatter": "default",
                        "level": "INFO",  # MINDSDB_CONSOLE_LOG_LEVEL or MINDSDB_LOG_LEVEL (obsolete)
                    },
                    "file": {
                        "enabled": False,
                        "level": "INFO",  # MINDSDB_FILE_LOG_LEVEL
                        "filename": "app.log",
                        "maxBytes": 1 << 19,  # 0.5 Mb
                        "backupCount": 3,
                    },
                }
            },
            "gui": {"autoupdate": True},
            "debug": False,
            "environment": "local",
            "integrations": {},
            "api": {
                "http": {
                    "host": api_host,
                    "port": "47334",
                    "restart_on_failure": True,
                    "max_restart_count": 1,
                    "max_restart_interval_seconds": 60,
                    "server": {
                        "type": "waitress",  # MINDSDB_HTTP_SERVER_TYPE MINDSDB_DEFAULT_SERVER
                        "config": {
                            "threads": 16,
                            "max_request_body_size": (1 << 30) * 10,  # 10GB
                            "inbuf_overflow": (1 << 30) * 10,
                        },
                    },
                },
                "mysql": {
                    "host": api_host,
                    "port": "47335",
                    "database": "mindsdb",
                    "ssl": True,
                    "restart_on_failure": True,
                    "max_restart_count": 1,
                    "max_restart_interval_seconds": 60,
                },
                "mongodb": {"host": api_host, "port": "47336", "database": "mindsdb"},
                "postgres": {"host": api_host, "port": "55432", "database": "mindsdb"},
                "mcp": {
                    "host": api_host,
                    "port": "47337",
                    "enabled": True,
                    "restart_on_failure": True,
                    "max_restart_count": 1,
                    "max_restart_interval_seconds": 60,
                },
                "litellm": {
                    "host": "0.0.0.0",  # API server binds to all interfaces by default
                    "port": "8000",
                },
                "a2a": {
                    "host": api_host,
                    "port": 47338,
                    "mindsdb_host": "localhost",
                    "mindsdb_port": 47334,
                    "agent_name": "my_agent",
                    "project_name": "mindsdb",
                    "enabled": False,
                },
            },
            "cache": {"type": "local"},
            "ml_task_queue": {"type": "local"},
            "url_file_upload": {"enabled": True, "allowed_origins": [], "disallowed_origins": []},
            "file_upload_domains": [],  # deprecated, use config[url_file_upload][allowed_origins] instead
            "web_crawling_allowed_sites": [],
            "cloud": False,
            "jobs": {"disable": False},
            "tasks": {"disable": False},
            "default_project": "mindsdb",
            "default_llm": {},
            "default_embedding_model": {},
            "default_reranking_model": {},
            "data_catalog": {
                "enabled": False,
            },
        }
        # endregion

        # region find 'auto' config file, create if not exists
        auto_config_name = "config.auto.json"
        auto_config_path = self.storage_root_path.joinpath(auto_config_name)
        if not auto_config_path.is_file():
            auto_config_path.write_text("{}")
        self.auto_config_path = auto_config_path
        # endregion

        self.prepare_env_config()

        self.fetch_auto_config()
        self.merge_configs()

        return cls.__instance

    def prepare_env_config(self) -> None:
        """Collect config values from env vars to self._env_config"""
        self._env_config = {
            "logging": {"handlers": {"console": {}, "file": {}}},
            "api": {"http": {"server": {}}, "a2a": {}},
            "auth": {},
            "paths": {},
            "permanent_storage": {},
            "ml_task_queue": {},
        }

        # region storage root path
        if os.environ.get("MINDSDB_STORAGE_DIR", "") != "":
            self._env_config["paths"] = {"root": Path(os.environ["MINDSDB_STORAGE_DIR"])}
        # endregion

        # region vars: permanent storage disabled?
        if os.environ.get("MINDSDB_STORAGE_BACKUP_DISABLED", "").lower() in (
            "1",
            "true",
        ):
            self._env_config["permanent_storage"] = {"location": "absent"}
        # endregion

        # region vars: ml queue
        if os.environ.get("MINDSDB_ML_QUEUE_TYPE", "").lower() == "redis":
            self._env_config["ml_task_queue"] = {
                "type": "redis",
                "host": os.environ.get("MINDSDB_ML_QUEUE_HOST", "localhost"),
                "port": int(os.environ.get("MINDSDB_ML_QUEUE_PORT", 6379)),
                "db": int(os.environ.get("MINDSDB_ML_QUEUE_DB", 0)),
                "username": os.environ.get("MINDSDB_ML_QUEUE_USERNAME"),
                "password": os.environ.get("MINDSDB_ML_QUEUE_PASSWORD"),
            }
        # endregion

        # region vars: username and password
        http_username = os.environ.get("MINDSDB_USERNAME")
        http_password = os.environ.get("MINDSDB_PASSWORD")

        if bool(http_username) != bool(http_password):
            raise ValueError(
                "Both MINDSDB_USERNAME and MINDSDB_PASSWORD must be set together and must be non-empty strings."
            )

        # If both username and password are set, enable HTTP auth.
        if http_username and http_password:
            self._env_config["auth"]["http_auth_enabled"] = True
            self._env_config["auth"]["username"] = http_username
            self._env_config["auth"]["password"] = http_password
        # endregion

        # region permanent session lifetime
        for env_name in (
            "MINDSDB_HTTP_PERMANENT_SESSION_LIFETIME",
            "FLASK_PERMANENT_SESSION_LIFETIME",
        ):
            env_value = os.environ.get(env_name)
            if isinstance(env_value, str):
                try:
                    permanent_session_lifetime = int(env_value)
                except Exception:
                    raise ValueError(f"Warning: Can't cast env var {env_name} value to int: {env_value}")
                self._env_config["auth"]["http_permanent_session_lifetime"] = permanent_session_lifetime
                break
        # endregion

        # region logging
        if os.environ.get("MINDSDB_LOG_LEVEL", "") != "":
            self._env_config["logging"]["handlers"]["console"]["level"] = os.environ["MINDSDB_LOG_LEVEL"]
            self._env_config["logging"]["handlers"]["console"]["enabled"] = True
        if os.environ.get("MINDSDB_CONSOLE_LOG_LEVEL", "") != "":
            self._env_config["logging"]["handlers"]["console"]["level"] = os.environ["MINDSDB_CONSOLE_LOG_LEVEL"]
            self._env_config["logging"]["handlers"]["console"]["enabled"] = True
        if os.environ.get("MINDSDB_FILE_LOG_LEVEL", "") != "":
            self._env_config["logging"]["handlers"]["file"]["level"] = os.environ["MINDSDB_FILE_LOG_LEVEL"]
            self._env_config["logging"]["handlers"]["file"]["enabled"] = True
        # endregion

        # region server type
        server_type = os.environ.get("MINDSDB_HTTP_SERVER_TYPE", "").lower()
        if server_type == "":
            server_type = os.environ.get("MINDSDB_DEFAULT_SERVER", "").lower()
        if server_type != "":
            if server_type == "waitress":
                self._env_config["api"]["http"]["server"]["type"] = "waitress"
                self._default_config["api"]["http"]["server"]["config"] = {}
                self._env_config["api"]["http"]["server"]["config"] = {
                    "threads": 16,
                    "max_request_body_size": (1 << 30) * 10,  # 10GB
                    "inbuf_overflow": (1 << 30) * 10,
                }
            elif server_type == "flask":
                self._env_config["api"]["http"]["server"]["type"] = "flask"
                self._default_config["api"]["http"]["server"]["config"] = {}
                self._env_config["api"]["http"]["server"]["config"] = {}
            elif server_type == "gunicorn":
                self._env_config["api"]["http"]["server"]["type"] = "gunicorn"
                self._default_config["api"]["http"]["server"]["config"] = {}
                self._env_config["api"]["http"]["server"]["config"] = {
                    "workers": min(mp.cpu_count(), 4),
                    "timeout": 600,
                    "reuse_port": True,
                    "preload_app": True,
                    "threads": 4,
                }
        # endregion

        if os.environ.get("MINDSDB_DB_CON", "") != "":
            self._env_config["storage_db"] = os.environ["MINDSDB_DB_CON"]

        if os.environ.get("MINDSDB_DEFAULT_PROJECT", "") != "":
            self._env_config["default_project"] = os.environ["MINDSDB_DEFAULT_PROJECT"].lower()

        if os.environ.get("MINDSDB_DEFAULT_LLM_API_KEY", "") != "":
            self._env_config["default_llm"] = {"api_key": os.environ["MINDSDB_DEFAULT_LLM_API_KEY"]}
        if os.environ.get("MINDSDB_DEFAULT_EMBEDDING_MODEL_API_KEY", "") != "":
            self._env_config["default_embedding_model"] = {
                "api_key": os.environ["MINDSDB_DEFAULT_EMBEDDING_MODEL_API_KEY"]
            }
        if os.environ.get("MINDSDB_DEFAULT_RERANKING_MODEL_API_KEY", "") != "":
            self._env_config["default_reranking_model"] = {
                "api_key": os.environ["MINDSDB_DEFAULT_RERANKING_MODEL_API_KEY"]
            }
        if os.environ.get("MINDSDB_DATA_CATALOG_ENABLED", "").lower() in ("1", "true"):
            self._env_config["data_catalog"] = {"enabled": True}

        # region vars: a2a configuration
        a2a_config = {}
        if os.environ.get("MINDSDB_A2A_HOST"):
            a2a_config["host"] = os.environ.get("MINDSDB_A2A_HOST")
        if os.environ.get("MINDSDB_A2A_PORT"):
            a2a_config["port"] = int(os.environ.get("MINDSDB_A2A_PORT"))
        if os.environ.get("MINDSDB_HOST"):
            a2a_config["mindsdb_host"] = os.environ.get("MINDSDB_HOST")
        if os.environ.get("MINDSDB_PORT"):
            a2a_config["mindsdb_port"] = int(os.environ.get("MINDSDB_PORT"))
        if os.environ.get("MINDSDB_AGENT_NAME"):
            a2a_config["agent_name"] = os.environ.get("MINDSDB_AGENT_NAME")
        if os.environ.get("MINDSDB_PROJECT_NAME"):
            a2a_config["project_name"] = os.environ.get("MINDSDB_PROJECT_NAME")
        if os.environ.get("MINDSDB_A2A_ENABLED") is not None:
            a2a_config["enabled"] = os.environ.get("MINDSDB_A2A_ENABLED").lower() in (
                "true",
                "1",
                "yes",
                "y",
            )

        if a2a_config:
            self._env_config["api"]["a2a"] = a2a_config
        # endregion

    def fetch_auto_config(self) -> bool:
        """Load dict readed from config.auto.json to `auto_config`.
        Do it only if `auto_config` was not loaded before or config.auto.json been changed.

        Returns:
            bool: True if config was loaded or updated
        """

        if (
            self.auto_config_path.is_file()
            and self.auto_config_path.read_text() != ""
            and self.auto_config_mtime != self.auto_config_path.stat().st_mtime
        ):
            try:
                self._auto_config = json.loads(self.auto_config_path.read_text())
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"The 'auto' configuration file ({self.auto_config_path}) contains invalid JSON: {e}\nFile content: {self.auto_config_path.read_text()}"
                )
            self.auto_config_mtime = self.auto_config_path.stat().st_mtime
            return True
        return False

    def fetch_user_config(self) -> bool:
        """Read config provided by the user to `user_config`. Do it only if `user_config` was not loaded before.

        Returns:
            bool: True if config was loaded
        """
        if self._user_config is None:
            cmd_args_config = self.cmd_args.config
            if isinstance(cmd_args_config, str):
                self.config_path = cmd_args_config
            elif isinstance(os.environ.get("MINDSDB_CONFIG_PATH"), str):
                self.config_path = os.environ["MINDSDB_CONFIG_PATH"]
            if self.config_path == "absent":
                self.config_path = None
            if isinstance(self.config_path, str):
                self.config_path = Path(self.config_path)
                if not self.config_path.is_file():
                    raise FileNotFoundError(f"The configuration file was not found at the path: {self.config_path}")
                try:
                    self._user_config = json.loads(self.config_path.read_text())
                except json.JSONDecodeError as e:
                    raise ValueError(f"The configuration file ({self.config_path}) contains invalid JSON: {e}")
            else:
                self._user_config = {}
            return True
        return False

    def ensure_auto_config_is_relevant(self) -> None:
        """Check if auto config has not been changed. If changed - reload main config."""
        updated = self.fetch_auto_config()
        if updated:
            self.merge_configs()

    def merge_configs(self) -> None:
        """Merge multiple configs to one."""
        new_config = deepcopy(self._default_config)
        _merge_configs(new_config, self._user_config)
        _merge_configs(new_config, self._auto_config or {})
        _merge_configs(new_config, self._env_config or {})

        # Apply command-line arguments for A2A
        a2a_config = {}

        # Check for A2A command-line arguments
        if hasattr(self.cmd_args, "a2a_host") and self.cmd_args.a2a_host is not None:
            a2a_config["host"] = self.cmd_args.a2a_host

        if hasattr(self.cmd_args, "a2a_port") and self.cmd_args.a2a_port is not None:
            a2a_config["port"] = self.cmd_args.a2a_port

        if hasattr(self.cmd_args, "mindsdb_host") and self.cmd_args.mindsdb_host is not None:
            a2a_config["mindsdb_host"] = self.cmd_args.mindsdb_host

        if hasattr(self.cmd_args, "mindsdb_port") and self.cmd_args.mindsdb_port is not None:
            a2a_config["mindsdb_port"] = self.cmd_args.mindsdb_port

        if hasattr(self.cmd_args, "agent_name") and self.cmd_args.agent_name is not None:
            a2a_config["agent_name"] = self.cmd_args.agent_name

        if hasattr(self.cmd_args, "project_name") and self.cmd_args.project_name is not None:
            a2a_config["project_name"] = self.cmd_args.project_name

        # Merge command-line args config with highest priority
        if a2a_config:
            _merge_configs(new_config, {"api": {"a2a": a2a_config}})

        # Ensure A2A port is never 0, which would prevent the A2A API from starting
        a2a_config = new_config["api"].get("a2a")
        if a2a_config is not None and isinstance(a2a_config, dict):
            if "port" in a2a_config and (a2a_config["port"] == 0 or a2a_config["port"] is None):
                a2a_config["port"] = 47338  # Use the default port value

        # region create dirs
        for key, value in new_config["paths"].items():
            if isinstance(value, str):
                new_config["paths"][key] = Path(value)
            elif isinstance(value, Path) is False:
                raise ValueError(f"Unexpected path value: {value}")
            create_data_dir(new_config["paths"][key])
        # endregion

        self._config = new_config

    def __getitem__(self, key):
        self.ensure_auto_config_is_relevant()
        return self._config[key]

    def get(self, key, default=None):
        self.ensure_auto_config_is_relevant()
        return self._config.get(key, default)

    def get_all(self):
        self.ensure_auto_config_is_relevant()
        return self._config

    def update(self, data: dict, overwrite: bool = False) -> None:
        """
        Update values in `auto` config.
        Args:
            data (dict): data to update in `auto` config.
            overwrite (bool): if True, overwrite existing keys, otherwise merge them.
                - False (default): Merge recursively. Existing nested dictionaries are preserved
                and only the specified keys in `data` are updated.
                - True: Overwrite completely. Existing keys are replaced entirely with values
                from `data`, discarding any nested structure not present in `data`.
        """
        self.ensure_auto_config_is_relevant()

        if overwrite:
            _overwrite_configs(self._auto_config, data)
        else:
            _merge_configs(self._auto_config, data)

        self.auto_config_path.write_text(json.dumps(self._auto_config, indent=4))

        self.auto_config_mtime = self.auto_config_path.stat().st_mtime

        self.merge_configs()

    def raise_warnings(self, logger) -> None:
        """Show warnings about config options"""

        if "storage_dir" in self._config:
            logger.warning("The 'storage_dir' config option is no longer supported. Use 'paths.root' instead.")

        if "log" in self._config:
            logger.warning("The 'log' config option is no longer supported. Use 'logging' instead.")

        if os.environ.get("MINDSDB_DEFAULT_SERVER", "") != "":
            logger.warning(
                "Env variable 'MINDSDB_DEFAULT_SERVER' is going to be deprecated soon. "
                "Use 'MINDSDB_HTTP_SERVER_TYPE' instead."
            )

        file_upload_domains = self._config.get("file_upload_domains")
        if isinstance(file_upload_domains, list) and len(file_upload_domains) > 0:
            allowed_origins = self._config["url_file_upload"]["allowed_origins"]
            if isinstance(allowed_origins, list) and len(allowed_origins) == 0:
                self._config["url_file_upload"]["allowed_origins"] = file_upload_domains
            logger.warning(
                'Config option "file_upload_domains" is deprecated, '
                'use config["url_file_upload"]["allowed_origins"] instead.'
            )

        for env_name in ("MINDSDB_HTTP_SERVER_TYPE", "MINDSDB_DEFAULT_SERVER"):
            env_value = os.environ.get(env_name, "")
            if env_value.lower() not in ("waitress", "flask", "gunicorn", ""):
                logger.warning(
                    f"The value '{env_value}' of the environment variable {env_name} is not valid. "
                    "It must be one of the following: 'waitress', 'flask', or 'gunicorn'."
                )

    @property
    def cmd_args(self):
        if self._cmd_args is None:
            self.parse_cmd_args()
        return self._cmd_args

    def parse_cmd_args(self) -> None:
        """Collect cmd args to self._cmd_args (accessable as self.cmd_args)"""
        if self._cmd_args is not None:
            return

        # if it is not mindsdb run, then set args to empty
        if (sys.modules["__main__"].__package__ or "").lower() != "mindsdb" and os.environ.get(
            "MINDSDB_RUNTIME"
        ) != "1":
            self._cmd_args = argparse.Namespace(
                api=None,
                config=None,
                install_handlers=None,
                verbose=False,
                no_studio=False,
                version=False,
                ml_task_queue_consumer=None,
                agent=None,
                project=None,
                update_gui=False,
            )
            return

        parser = argparse.ArgumentParser(description="CL argument for mindsdb server")
        parser.add_argument("--api", type=str, default=None)
        parser.add_argument("--config", type=str, default=None)
        parser.add_argument("--install-handlers", type=str, default=None)
        parser.add_argument("--verbose", action="store_true")
        parser.add_argument("--no_studio", action="store_true")
        parser.add_argument("-v", "--version", action="store_true")
        parser.add_argument("--ml_task_queue_consumer", action="store_true", default=None)
        parser.add_argument(
            "--agent",
            type=str,
            default=None,
            help="Name of the agent to use with litellm APIs",
        )
        parser.add_argument(
            "--project",
            type=str,
            default=None,
            help="Project containing the agent (default: mindsdb)",
        )

        # A2A specific arguments
        parser.add_argument("--a2a-host", type=str, default=None, help="A2A server host")
        parser.add_argument("--a2a-port", type=int, default=None, help="A2A server port")
        parser.add_argument("--mindsdb-host", type=str, default=None, help="MindsDB server host")
        parser.add_argument("--mindsdb-port", type=int, default=None, help="MindsDB server port")
        parser.add_argument(
            "--agent-name",
            type=str,
            default=None,
            help="MindsDB agent name to connect to",
        )
        parser.add_argument("--project-name", type=str, default=None, help="MindsDB project name")
        parser.add_argument("--update-gui", action="store_true", default=False, help="Update GUI and exit")

        self._cmd_args = parser.parse_args()

    @property
    def paths(self):
        return self._config["paths"]

    @property
    def user_config(self):
        return self._user_config

    @property
    def auto_config(self):
        return self._auto_config

    @property
    def env_config(self):
        return self._env_config

    @property
    def is_cloud(self):
        return self._config.get("cloud", False)


config = Config()
