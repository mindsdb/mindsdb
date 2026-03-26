import re
import os
import json
import logging
import threading
from typing import Any
from logging.config import dictConfig

from mindsdb.utilities.config import config as app_config


logging_initialized = False


class JsonFormatter(logging.Formatter):
    def format(self, record):
        record_message = super().format(record)
        log_record = {
            "process_name": record.processName,
            "name": record.name,
            "message": record_message,
            "level": record.levelname,
            "time": record.created,
        }
        return json.dumps(log_record)


class ColorFormatter(logging.Formatter):
    green = "\x1b[32;20m"
    default = "\x1b[39;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s %(processName)15s %(levelname)-8s %(name)s: %(message)s"

    FORMATS = {
        logging.DEBUG: logging.Formatter(green + format + reset),
        logging.INFO: logging.Formatter(default + format + reset),
        logging.WARNING: logging.Formatter(yellow + format + reset),
        logging.ERROR: logging.Formatter(red + format + reset),
        logging.CRITICAL: logging.Formatter(bold_red + format + reset),
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        return log_fmt.format(record)


FORMATTERS = {
    "default": {"()": ColorFormatter},
    "json": {"()": JsonFormatter},
    "file": {"format": "%(asctime)s %(processName)15s %(levelname)-8s %(name)s: %(message)s"},
}


class LogSanitizer:
    """Log Sanitizer"""

    SENSITIVE_KEYS = {
        "password",
        "passwd",
        "pwd",
        "token",
        "access_token",
        "refresh_token",
        "bearer_token",
        "api_key",
        "apikey",
        "api-key",
        "openai_api_key",
        "secret",
        "secret_key",
        "client_secret",
        "credentials",
        "auth",
        "authorization",
        "private_key",
        "private-key",
        "session_id",
        "sessionid",
        "credit_card",
        "card_number",
        "cvv",
    }

    def __init__(self, mask: str | None = None):
        self.mask = mask or "********"
        self._compile_patterns()

    def _compile_patterns(self):
        self.search_pattern = re.compile(
            r"\b(" + "|".join(re.escape(key) for key in self.SENSITIVE_KEYS) + r")\b", re.IGNORECASE
        )
        self.patterns = []
        for key in self.SENSITIVE_KEYS:
            # Patterns for: key=value, key: value, "key": "value", 'key': 'value'
            # Note: negative lookahead (?!%) excludes Python format placeholders like %s, %d, etc.
            patterns = [
                re.compile(f'{key}["\s]*[:=]["\s]*(?!%)([^\s,}}\\]"\n]+)', re.IGNORECASE),
                re.compile(f'"{key}"["\s]*:["\s]*"([^"]+)"', re.IGNORECASE),
                re.compile(f"'{key}'['\s]*:['\s]*'([^']+)'", re.IGNORECASE),
            ]
            self.patterns.extend(patterns)

    def _replace(self, m) -> str:
        return m.group(0).replace(m.group(1), self.mask)

    def sanitize_text(self, text: str) -> str:
        if self.search_pattern.search(text):
            for pattern in self.patterns:
                text = pattern.sub(self._replace, text)
        return text

    def sanitize_dict(self, data: dict) -> dict:
        if not isinstance(data, dict):
            return data

        sanitized = {}
        for key, value in data.items():
            if any(sensitive in str(key).lower() for sensitive in self.SENSITIVE_KEYS):
                sanitized[key] = self.mask
            elif isinstance(value, dict):
                sanitized[key] = self.sanitize_dict(value)
            elif isinstance(value, list):
                sanitized[key] = [self.sanitize_dict(item) if isinstance(item, dict) else item for item in value]
            else:
                sanitized[key] = value
        return sanitized

    def sanitize(self, data: Any) -> Any:
        if isinstance(data, dict):
            return self.sanitize_dict(data)
        elif isinstance(data, str):
            return self.sanitize_text(data)
        elif isinstance(data, (list, tuple)):
            return type(data)(self.sanitize(item) for item in data)
        return data


class SanitizingMixin:
    """Mixin for sanitizing log records."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sanitizer = LogSanitizer()

    def sanitize_record(self, record):
        """Sanitize a log record before emitting."""
        if (
            hasattr(record, "args")
            and isinstance(record.args, (list, tuple))
            and len(record.args) > 0
            and isinstance(record.msg, str)
        ):
            record.msg = record.msg % record.args
            record.args = []

        if isinstance(record.msg, str):
            record.msg = self.sanitizer.sanitize_text(record.msg)
        elif isinstance(record.msg, dict):
            record.msg = self.sanitizer.sanitize_dict(record.msg)

        if hasattr(record, "args") and record.args:
            record.args = self.sanitizer.sanitize(record.args)

        return record


class StreamSanitizingHandler(SanitizingMixin, logging.StreamHandler):
    def emit(self, record):
        record = self.sanitize_record(record)
        super().emit(record)


class FileSanitizingHandler(SanitizingMixin, logging.handlers.RotatingFileHandler):
    def emit(self, record):
        record = self.sanitize_record(record)
        super().emit(record)


def get_console_handler_config_level() -> int:
    console_handler_config = app_config["logging"]["handlers"]["console"]
    return getattr(logging, console_handler_config["level"])


def get_file_handler_config_level() -> int:
    file_handler_config = app_config["logging"]["handlers"]["file"]
    return getattr(logging, file_handler_config["level"])


def get_mindsdb_log_level() -> int:
    console_handler_config_level = get_console_handler_config_level()
    file_handler_config_level = get_file_handler_config_level()

    return min(console_handler_config_level, file_handler_config_level)


def get_handlers_config(process_name: str) -> dict:
    handlers_config = {}
    console_handler_config = app_config["logging"]["handlers"]["console"]
    console_handler_config_level = getattr(logging, console_handler_config["level"])
    if console_handler_config["enabled"] is True:
        handlers_config["console"] = {
            "class": "mindsdb.utilities.log.StreamSanitizingHandler",
            "formatter": console_handler_config.get("formatter", "default"),
            "level": console_handler_config_level,
        }

    file_handler_config = app_config["logging"]["handlers"]["file"]
    file_handler_config_level = getattr(logging, file_handler_config["level"])
    if file_handler_config["enabled"] is True:
        file_name = file_handler_config["filename"]
        if process_name is not None:
            if "." in file_name:
                parts = file_name.rpartition(".")
                file_name = f"{parts[0]}_{process_name}.{parts[2]}"
            else:
                file_name = f"{file_name}_{process_name}"
        handlers_config["file"] = {
            "class": "mindsdb.utilities.log.FileSanitizingHandler",
            "formatter": "file",
            "level": file_handler_config_level,
            "filename": app_config.paths["log"] / file_name,
            "maxBytes": file_handler_config["maxBytes"],  # 0.5 Mb
            "backupCount": file_handler_config["backupCount"],
        }
    return handlers_config


def configure_logging(process_name: str = None):
    handlers_config = get_handlers_config(process_name)
    mindsdb_log_level = get_mindsdb_log_level()

    logging_config = dict(
        version=1,
        formatters=FORMATTERS,
        handlers=handlers_config,
        loggers={
            "": {  # root logger
                "handlers": list(handlers_config.keys()),
                "level": mindsdb_log_level,
            },
            "__main__": {
                "level": mindsdb_log_level,
            },
            "mindsdb": {
                "level": mindsdb_log_level,
            },
            "alembic": {
                "level": mindsdb_log_level,
            },
        },
    )

    dictConfig(logging_config)


def initialize_logging(process_name: str = None) -> None:
    """Initialyze logging"""
    global logging_initialized
    if not logging_initialized:
        configure_logging(process_name)
        logging_initialized = True


# I would prefer to leave code to use logging.getLogger(), but there are a lot of complicated situations
# in MindsDB with processes being spawned that require logging to be configured again in a lot of cases.
# Using a custom logger-getter like this lets us do that logic here, once.
def getLogger(name=None):
    """
    Get a new logger, configuring logging first if it hasn't been done yet.
    """
    initialize_logging()
    return logging.getLogger(name)


def log_ram_info(logger: logging.Logger) -> None:
    """Log RAM/memory information to the provided logger.

    This function logs memory usage information: total, available, used memory in GB and memory
    usage percentage. The logging only occurs if the logger is enabled for DEBUG level.

    Args:
        logger (logging.Logger): The logger instance to use for outputting memory information.
    """
    if logger.isEnabledFor(logging.DEBUG) is False:
        return

    try:
        import psutil

        memory = psutil.virtual_memory()
        total_memory_gb = memory.total / (1024**3)
        available_memory_gb = memory.available / (1024**3)
        used_memory_gb = memory.used / (1024**3)
        memory_percent = memory.percent
        logger.debug(
            f"Memory: {total_memory_gb:.1f}GB total, {available_memory_gb:.1f}GB available, {used_memory_gb:.1f}GB used ({memory_percent:.1f}%)"
        )
    except Exception as e:
        logger.debug(f"Failed to get memory information: {e}")


def log_system_info(logger: logging.Logger) -> None:
    """Log detailed system information for debugging purposes.

    The function only logs system information (if the logger is configured for DEBUG level):
    - Operating system details (OS type, version, distribution, architecture)
    - CPU information (processor type, physical and logical core counts)
    - Memory information (total, available, used memory in GB and percentage)
    - GPU information (NVIDIA, AMD, Intel graphics cards with memory details)

    Args:
        logger (logging.Logger): The logger instance to use for outputting system information.
                                 Must be configured for DEBUG level to see the output.

    Returns:
        None

    Note:
        - For Linux systems, attempts to detect distribution via /etc/os-release, /etc/issue, or lsb_release
        - For Windows systems, uses wmic commands to get detailed OS and GPU information
        - For macOS systems, uses sw_vers and system_profiler commands
        - GPU detection supports NVIDIA (via nvidia-smi), AMD (via rocm-smi), and fallback methods
        - All subprocess calls have timeout protection to prevent hanging
        - If any system information gathering fails, it logs the error and continues
    """
    if logger.isEnabledFor(logging.DEBUG) is False:
        return

    try:
        import os
        import shutil
        import psutil
        import platform
        import subprocess

        # region OS information
        os_system = platform.system()
        os_release = platform.release()
        os_machine = platform.machine()

        os_details = []

        if os_system == "Linux":
            # Try to detect Linux distribution
            distro_info = "Unknown Linux"
            try:
                # Check for /etc/os-release (most modern distributions)
                if os.path.exists("/etc/os-release"):
                    with open("/etc/os-release", "r") as f:
                        os_release_data = {}
                        for line in f:
                            if "=" in line:
                                key, value = line.strip().split("=", 1)
                                os_release_data[key] = value.strip('"')

                        if "PRETTY_NAME" in os_release_data:
                            distro_info = os_release_data["PRETTY_NAME"]
                        elif "NAME" in os_release_data and "VERSION" in os_release_data:
                            distro_info = f"{os_release_data['NAME']} {os_release_data['VERSION']}"
                        elif "ID" in os_release_data:
                            distro_info = os_release_data["ID"].title()
                # Fallback to /etc/issue
                elif os.path.exists("/etc/issue"):
                    with open("/etc/issue", "r") as f:
                        issue_content = f.read().strip()
                        if issue_content:
                            distro_info = issue_content.split("\n")[0]
                # Fallback to lsb_release
                else:
                    try:
                        result = subprocess.run(["lsb_release", "-d"], capture_output=True, text=True, timeout=2)
                        if result.returncode == 0:
                            distro_info = result.stdout.split(":")[-1].strip()
                    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                        pass
            except Exception:
                pass

            os_details.append(f"{distro_info} (kernel {os_release})")

        elif os_system == "Windows":
            os_name = "Windows"
            os_version = "unknown"
            try:
                result = subprocess.run(
                    ["wmic", "os", "get", "Caption,Version", "/format:list"], capture_output=True, text=True, timeout=3
                )
                if result.returncode == 0:
                    windows_info = {}
                    for line in result.stdout.strip().split("\n"):
                        if "=" in line:
                            key, value = line.strip().split("=", 1)
                            windows_info[key] = value.strip()

                    if "Caption" in windows_info and "Version" in windows_info:
                        os_name = windows_info["Caption"]
                        os_version = windows_info["Version"]
            except Exception:
                pass
            os_details.append(f"{os_name} {os_release} (version {os_version})")

        elif os_system == "Darwin":  # macOS
            os_name = "macOS"
            os_version = "unknown"
            try:
                result = subprocess.run(
                    ["sw_vers", "-productName", "-productVersion"], capture_output=True, text=True, timeout=3
                )
                if result.returncode == 0:
                    lines = result.stdout.strip().split("\n")
                    if len(lines) >= 2:
                        os_name = lines[0].strip()
                        os_version = lines[1].strip()
            except Exception:
                pass
            os_details.append(f"{os_name} {os_release} (version {os_version})")
        else:
            os_details.append(f"{os_system} {os_release}")

        os_details.append(f"({os_machine})")
        os_info = " ".join(os_details)
        logger.debug(f"Operating System: {os_info}")
        # endregion

        # region CPU information
        cpu_info = platform.processor()
        if not cpu_info or cpu_info == "":
            cpu_info = platform.machine()
        cpu_count = psutil.cpu_count(logical=False)
        cpu_count_logical = psutil.cpu_count(logical=True)
        logger.debug(f"CPU: {cpu_info} ({cpu_count} physical cores, {cpu_count_logical} logical cores)")
        # endregion

        # memory information
        log_ram_info(logger)

        # region GPU information
        gpu_info = []
        try:
            # Check for NVIDIA GPU (works on Linux, Windows, macOS)
            nvidia_smi_path = shutil.which("nvidia-smi")
            if nvidia_smi_path:
                try:
                    result = subprocess.run(
                        [nvidia_smi_path, "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
                        capture_output=True,
                        text=True,
                        timeout=3,
                    )
                    if result.returncode == 0:
                        for line in result.stdout.strip().split("\n"):
                            if line.strip():
                                parts = line.split(", ")
                                if len(parts) >= 2:
                                    gpu_name = parts[0].strip()
                                    gpu_memory = parts[1].strip()
                                    gpu_info.append(f"{gpu_name} ({gpu_memory}MB)")
                except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                    pass

            # Check for AMD GPU (rocm-smi on Linux, wmic on Windows)
            if not gpu_info:  # Only check AMD if no NVIDIA GPU found
                if platform.system() == "Windows":
                    # Use wmic on Windows to detect AMD GPU
                    try:
                        result = subprocess.run(
                            ["wmic", "path", "win32_VideoController", "get", "name"],
                            capture_output=True,
                            text=True,
                            timeout=3,
                        )
                        if result.returncode == 0:
                            for line in result.stdout.strip().split("\n"):
                                line = line.strip()
                                if line and line != "Name" and "AMD" in line.upper():
                                    gpu_info.append(line)
                    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                        pass
                else:
                    # Use rocm-smi on Linux/macOS
                    rocm_smi_path = shutil.which("rocm-smi")
                    if rocm_smi_path:
                        try:
                            result = subprocess.run(
                                [rocm_smi_path, "--showproductname"], capture_output=True, text=True, timeout=3
                            )
                            if result.returncode == 0:
                                for line in result.stdout.strip().split("\n"):
                                    if "Product Name" in line:
                                        gpu_name = line.split(":")[-1].strip()
                                        gpu_info.append(gpu_name)
                        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                            pass

            # Fallback: Try to detect any GPU using platform-specific methods
            if not gpu_info:
                if platform.system() == "Windows":
                    try:
                        # Use wmic to get all video controllers
                        result = subprocess.run(
                            ["wmic", "path", "win32_VideoController", "get", "name"],
                            capture_output=True,
                            text=True,
                            timeout=3,
                        )
                        if result.returncode == 0:
                            for line in result.stdout.strip().split("\n"):
                                line = line.strip()
                                if (
                                    line
                                    and line != "Name"
                                    and any(
                                        keyword in line.upper()
                                        for keyword in ["NVIDIA", "AMD", "INTEL", "RADEON", "GEFORCE"]
                                    )
                                ):
                                    gpu_info.append(line)
                    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                        pass
                elif platform.system() == "Darwin":  # macOS
                    try:
                        # Use system_profiler on macOS
                        result = subprocess.run(
                            ["system_profiler", "SPDisplaysDataType"], capture_output=True, text=True, timeout=3
                        )
                        if result.returncode == 0:
                            for line in result.stdout.strip().split("\n"):
                                if "Chipset Model:" in line:
                                    gpu_name = line.split(":")[-1].strip()
                                    gpu_info.append(gpu_name)
                    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                        pass

        except Exception:
            pass

        if gpu_info:
            logger.debug(f"GPU: {', '.join(gpu_info)}")
        else:
            logger.debug("GPU: Not detected or not supported")
        # endregion

    except Exception as e:
        logger.debug(f"Failed to get system information: {e}")


def resources_log_thread(stop_event: threading.Event, interval: int = 60):
    """Log resources information to the logger

    Args:
        stop_event (Event): Event to stop the thread
        interval (int): Interval in seconds to log resources information

    Returns:
        None

    Note:
        Output shows:
            - RAM: total, available, used memory in GB and memory usage percentage
            - Consumed RAM: sum of rss, and percentage of total memory used
            - CPU usage: average CPU usage for last period
            - Active queries: number of active SQL queries
    """
    from mindsdb.utilities.fs import get_tmp_dir

    logger = getLogger(__name__)
    while stop_event.wait(timeout=interval) is False:
        try:
            import psutil

            main_process = psutil.Process(os.getpid())
            children = main_process.children(recursive=True)

            total_memory_info = {
                "main_process": {
                    "pid": main_process.pid,
                    "name": main_process.name(),
                    "memory_info": main_process.memory_info(),
                    "memory_percent": main_process.memory_percent(),
                },
                "children": [],
                "total_memory": {"rss": 0, "vms": 0, "percent": 0},
            }

            for child in children:
                try:
                    child_info = {
                        "pid": child.pid,
                        "name": child.name(),
                        "memory_info": child.memory_info(),
                        "memory_percent": child.memory_percent(),
                    }
                    total_memory_info["children"].append(child_info)

                    total_memory_info["total_memory"]["rss"] += child.memory_info().rss
                    total_memory_info["total_memory"]["vms"] += child.memory_info().vms
                    total_memory_info["total_memory"]["percent"] += child.memory_percent()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            total_memory_info["total_memory"]["rss"] += main_process.memory_info().rss
            total_memory_info["total_memory"]["vms"] += main_process.memory_info().vms
            total_memory_info["total_memory"]["percent"] += main_process.memory_percent()

            memory = psutil.virtual_memory()
            total_memory_gb = memory.total / (1024**3)
            available_memory_gb = memory.available / (1024**3)
            used_memory_gb = memory.used / (1024**3)
            memory_percent = memory.percent
            cpu_usage = psutil.cpu_percent()

            active_http_queries = 0
            p = get_tmp_dir().joinpath("processes/http_query/")
            if p.exists() and p.is_dir():
                for _ in p.iterdir():
                    active_http_queries += 1

            active_mysql_queries = 0
            p = get_tmp_dir().joinpath("processes/mysql_query/")
            if p.exists() and p.is_dir():
                for _ in p.iterdir():
                    active_mysql_queries += 1

            level = app_config["logging"]["resources_log"]["level"]
            logger.log(
                logging.getLevelName(level),
                f"RAM: {total_memory_gb:.1f}GB total, {available_memory_gb:.1f}GB available, {used_memory_gb:.1f}GB used ({memory_percent:.1f}%)\n"
                f"Consumed RAM: {total_memory_info['total_memory']['rss'] / (1024**2):.1f}Mb, {total_memory_info['total_memory']['percent']:.2f}%\n"
                f"CPU usage: {cpu_usage}% {interval}s\n"
                f"Active queries: {active_http_queries}/HTTP {active_mysql_queries}/MySQL",
            )
        except Exception as e:
            logger.debug(f"Failed to get memory information: {e}")
