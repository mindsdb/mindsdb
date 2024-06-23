import os
import sys
import time
import json
import shutil
import subprocess
from pathlib import Path

import docker
import pytest
import psutil
import requests
import netifaces
import pandas as pd
from walrus import Database

from mindsdb.utilities import log
from mindsdb.utilities.ps import get_child_pids


logger = log.getLogger(__name__)

HTTP_API_ROOT = 'http://127.0.0.1:47334/api'
USE_PERSISTENT_STORAGE = bool(int(os.getenv('USE_PERSISTENT_STORAGE') or "0"))
TEST_CONFIG = os.path.dirname(os.path.realpath(__file__)) + '/config/config.json'
TEMP_DIR = Path(__file__).parent.absolute().joinpath('../../').joinpath(
    f'temp/test_storage_{int(time.time()*1000)}/' if not USE_PERSISTENT_STORAGE else 'temp/test_storage/'
).resolve()
TEMP_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = TEMP_DIR.joinpath('config.json')


def remove_container(container_name: str):
    """ Stop and remove docker contaier

        Args:
            container_name (str): name of conteiner to remove
    """
    try:
        docker_client = docker.from_env()
        container = docker_client.containers.get(container_name)
        try:
            container.kill()
        except Exception:
            pass
        container.remove()
    except docker.errors.NotFound:
        pass


def make_test_csv(name, data):
    test_csv_path = TEMP_DIR.joinpath(f'{name}.csv').resolve()
    df = pd.DataFrame(data)
    df.to_csv(test_csv_path, index=False)
    return str(test_csv_path)


def docker_inet_ip():
    if os.environ.get("MICROSERVICE_MODE", False):
        return "127.0.0.1"

    """Get ip of docker0 interface."""
    if "docker0" not in netifaces.interfaces():
        raise Exception("Unable to find 'docker' interface. Please install docker first.")
    return netifaces.ifaddresses('docker0')[netifaces.AF_INET][0]['addr']


@pytest.fixture(scope="session")
def temp_dir():
    """Create temp directory to store mindsdb data inside it.
    The directory is created once for the whole test session.
    See 'scope' fixture parameter
    """
    temp_dir = Path(__file__).parent.absolute().joinpath('../../').joinpath(
        f'temp/test_storage_{int(time.time()*1000)}/' if not USE_PERSISTENT_STORAGE else 'temp/test_storage/'
    ).resolve()
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


@pytest.fixture(scope="module")
def config(temp_dir):
    """Create config used by mindsdb app in tests.
    The config is created once for the whole test module.
    See 'scope' fixture parameter.
    """
    if os.environ.get("MICROSERVICE_MODE", False):
        config_json = {
            "api":
            {
                "http": {
                    "host": "127.0.0.1",
                    "port": 47334,
                },
                "mysql": {
                    "host": "127.0.0.1",
                    "port": 47335,
                },

                "mongodb": {
                    "host": "127.0.0.1",
                    "port": 47336,
                },
            }
        }
        return config_json

    with open(TEST_CONFIG, 'rt') as f:
        config_json = json.loads(f.read())
        config_json['storage_dir'] = f'{TEMP_DIR}'
        config_json['storage_db'] = f'sqlite:///{TEMP_DIR}/mindsdb.sqlite3.db?check_same_thread=False&timeout=30'
        config_json['integrations'] = {}

    return config_json


def override_recursive(a, b):
    """Overrides some elements in json 'a' by elements in json 'b'"""
    for key in b:
        if isinstance(b[key], dict) is False:
            a[key] = b[key]
        elif key not in a or isinstance(a[key], dict) is False:
            a[key] = b[key]
        # make config section empty by demand
        elif isinstance(b[key], dict) is True and b[key] == {}:
            a[key] = b[key]
        else:
            override_recursive(a[key], b[key])


@pytest.fixture(scope="module")
def mindsdb_app(request, config):
    """Start mindsdb app before tests and stop it after ones.
    Takes 'OVERRIDE_CONFIG' and 'API_LIST' from test module (file)
    """
    if os.environ.get("MICROSERVICE_MODE", False):
        cmd = ['docker-compose', '-f', './docker/docker-compose-ci.yml', 'up']
        timeout = 1800
    else:

        apis = getattr(request.module, "API_LIST", [])
        if not apis:
            api_str = "http,mysql"
        else:
            api_str = ",".join(apis)
        to_override_conf = getattr(request.module, "OVERRIDE_CONFIG", {})
        if to_override_conf:
            override_recursive(config, to_override_conf)
        config_path = TEMP_DIR.joinpath('config.json')
        with open(config_path, "wt") as f:
            f.write(json.dumps(config))

        os.environ['CHECK_FOR_UPDATES'] = '0'
        cmd = ['python3', '-m', 'mindsdb', f'--api={api_str}', f'--config={config_path}', '--verbose']
        if getattr(request.module, "USE_GUI", False) is False:
            cmd.append('--no_studio')
        if getattr(request.module, "ML_TASK_QUEUE_CONSUMER", False) is True:
            cmd.append('--ml_task_queue_consumer')
        timeout = 90

    print('Starting mindsdb process!')
    app = subprocess.Popen(
        cmd,
        close_fds=True,
        stdout=sys.stdout,
        stderr=sys.stderr,
        shell=False
    )
    threshold = time.time() + timeout

    while True:
        try:
            host = config["api"]["http"]["host"]
            port = config["api"]["http"]["port"]
            r = requests.get(f"http://{host}:{port}/api/util/ping")
            r.raise_for_status()
            break
        except Exception:
            time.sleep(1)
            if time.time() > threshold:
                raise Exception(f"unable to launch mindsdb app in {timeout} seconds")

    def cleanup():
        print("Stopping Application")
        if os.environ.get("MICROSERVICE_MODE", False):
            cmd = 'docker-compose -f ./docker/docker-compose-ci.yml down'
            subprocess.run(cmd, shell=True)
            # shutil.rmtree("./var")
        else:
            for ch in get_child_pids(app.pid):
                try:
                    ch.kill()
                except psutil.NoSuchProcess:
                    pass
            app.kill()
    request.addfinalizer(cleanup)
    yield
    # Clean up metrics in between test suites.
    try:
        prom_dir = os.getenv('PROMETHEUS_MULTIPROC_DIR')
        if prom_dir is None:
            # Nothing to clean up.
            return
        shutil.rmtree(prom_dir)
        os.mkdir(prom_dir)
    except Exception as e:
        logger.error(f'Unable to reset PROMETHEUS_MULTIPROC_DIR: {str(e)}')


def waitReadiness(container, match_msg, match_number=2, timeout=30):
    """Wait the container readiness.
    Args:
        match_msg: str - substring in log indicates container readiness
        match_number: int - how many times 'match_msg' needs to be found in container logs
        timeout: - timeout
    Raises:
        timeout exceeded error if the container wasn't ready in timeout.
    """
    threshold = time.time() + timeout
    ready_msg = match_msg
    while True:
        lines = container.logs().decode()
        # container fully ready
        # because it reloads the db server during initialization
        # need to check that the 'ready for connections' has found second time
        if lines.count(ready_msg) >= 2:
            break
        if time.time() > threshold:
            raise Exception("timeout exceeded, container is still not ready")


@pytest.fixture(scope="class")
def postgres_db(request):
    if os.environ.get("MICROSERVICE_MODE", False):
        connection_args = {
            "host": "postgres_db",
            "port": "5432",
            "user": "postgres",
            "password": "supersecret",
            "database": "test",
        }
    else:
        image_name = "mindsdb/postgres-handler-test"
        docker_client = docker.from_env()
        container = None

        connection_args = {
            "host": "localhost",
            "port": "15432",
            "user": "postgres",
            "password": "supersecret",
            "database": "test",
        }

        try:
            container = docker_client.containers.run(
                image_name,
                detach=True,
                environment={"POSTGRES_PASSWORD": "supersecret"},
                ports={"5432/tcp": 15432},
            )
            waitReadiness(container, "database system is ready to accept connections")
        except Exception as e:
            if container is not None:
                container.kill()
            raise e

    request.cls.postgres_db = {
        "type": "postgres",
        "connection_data": connection_args
    }

    yield

    if not os.environ.get("MICROSERVICE_MODE", False):
        container.kill()
        docker_client.close()


@pytest.fixture(scope="class")
def mysql_db(request):
    if os.environ.get("MICROSERVICE_MODE", False):
        connection_args = {
            "host": "mysql_db",
            "port": "13306",
            "user": "root",
            "password": "supersecret",
            "database": "test",
            "ssl": False
        }
    else:
        image_name = "mindsdb/mysql-handler-test"
        docker_client = docker.from_env()
        container = None

        connection_args = {
            "host": "localhost",
            "port": "13306",
            "user": "root",
            "password": "supersecret",
            "database": "test",
            "ssl": False
        }

        try:
            container = docker_client.containers.run(
                image_name,
                command="--secure-file-priv=/",
                detach=True,
                environment={"MYSQL_ROOT_PASSWORD": "supersecret"},
                ports={"3306/tcp": 13306}
            )
            waitReadiness(container, "/usr/sbin/mysqld: ready for connections. Version: '8.0.27'")
        except Exception as e:
            if container is not None:
                container.kill()
            raise e

    request.cls.mysql_db = {
        "type": "mysql",
        "connection_data": connection_args
    }

    yield

    if not os.environ.get("MICROSERVICE_MODE", False):
        container.kill()
        docker_client.close()


@pytest.fixture(scope="class")
def maria_db(request):
    if os.environ.get("MICROSERVICE_MODE", False):
        connection_args = {
            "host": "maria_db",
            "port": "3306",
            "user": "root",
            "password": "supersecret",
            "database": "test",
            "ssl": False
        }
    else:
        image_name = "mindsdb/mariadb-handler-test"
        docker_client = docker.from_env()
        container = None

        connection_args = {
            "host": "localhost",
            "port": "13307",
            "user": "root",
            "password": "supersecret",
            "database": "test",
            "ssl": False
        }

        try:
            container = docker_client.containers.run(
                image_name,
                command="--secure-file-priv=/",
                detach=True,
                environment={"MARIADB_ROOT_PASSWORD": "supersecret"},
                ports={"3306/tcp": 13307},
            )
            waitReadiness(container, "mariadbd: ready for connections")
        except Exception as e:
            if container is not None:
                container.kill()
            raise e

    request.cls.maria_db = {
        "type": "mariadb",
        "connection_data": connection_args
    }

    yield

    if not os.environ.get("MICROSERVICE_MODE", False):
        container.kill()
        docker_client.close()


@pytest.fixture(scope="module")
def redis():
    """ start redis docker contaienr
    """
    image_name = "redis:7.2.1"
    docker_client = docker.from_env()
    container_name = 'mindsdb-test-redis'

    try:
        remove_container(container_name)

        docker_client.containers.run(
            image_name,
            detach=True,
            network='host',
            name=container_name
        )

        # region check connection to redis
        db = Database(protocol=3)
        start_time = time.time()
        connected = False
        while (connected is False) and (time.time() - start_time < 30):
            try:
                connected = db.ping()
            except Exception:
                pass
            time.sleep(1)
        if connected is False:
            raise Exception("Cant conect to redis in 10s")
        # endregion

        yield
    except Exception as e:
        print(f'Got exception during redis container starting: {e}')
        raise
    finally:
        remove_container(container_name)
        docker_client.close()
