from mindsdb.utilities import log

logger = log.getLogger("mindsdb")
logger.debug("Starting MindsDB...")

import os
import sys
import time
import json
import atexit
import signal
import psutil
import asyncio
import secrets
import traceback
import threading
from packaging import version

from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.api.http.start import start as start_http
from mindsdb.api.mysql.start import start as start_mysql
from mindsdb.api.mongo.start import start as start_mongo
from mindsdb.api.postgres.start import start as start_postgres
from mindsdb.interfaces.tasks.task_monitor import start as start_tasks
from mindsdb.utilities.ml_task_queue.consumer import start as start_ml_task_queue
from mindsdb.interfaces.jobs.scheduler import start as start_scheduler
from mindsdb.utilities.config import Config
from mindsdb.utilities.ps import is_pid_listen_port, get_child_pids
from mindsdb.utilities.functions import args_parse, get_versions_where_predictors_become_obsolete
from mindsdb.interfaces.database.integrations import integration_controller
import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.utilities.install import install_dependencies
from mindsdb.utilities.fs import create_dirs_recursive, clean_process_marks, clean_unlinked_process_marks
from mindsdb.utilities.telemetry import telemetry_file_exists, disable_telemetry
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.auth import register_oauth_client, get_aws_meta_data
from mindsdb.utilities.sentry import sentry_sdk  # noqa: F401

try:
    import torch.multiprocessing as mp
except Exception:
    import multiprocessing as mp
try:
    mp.set_start_method('spawn')
except RuntimeError:
    logger.info('Torch multiprocessing context already set, ignoring...')


_stop_event = threading.Event()


def close_api_gracefully(apis):
    _stop_event.set()
    try:
        for api in apis.values():
            try:
                process = api['process']
                childs = get_child_pids(process.pid)
                for p in childs:
                    try:
                        os.kill(p, signal.SIGTERM)
                    except Exception:
                        p.kill()
                sys.stdout.flush()
                process.terminate()
                process.join()
                sys.stdout.flush()
            except psutil.NoSuchProcess:
                pass
    except KeyboardInterrupt:
        sys.exit(0)


def do_clean_process_marks():
    while _stop_event.wait(timeout=5) is False:
        clean_unlinked_process_marks()


if __name__ == '__main__':
    # warn if less than 1Gb of free RAM
    if psutil.virtual_memory().available < (1 << 30):
        logger.warning(
            'The system is running low on memory. '
            + 'This may impact the stability and performance of the program.'
        )

    clean_process_marks()
    ctx.set_default()
    args = args_parse()

    # ---- CHECK SYSTEM ----
    if not (sys.version_info[0] >= 3 and sys.version_info[1] >= 8):
        print("""
     MindsDB requires Python >= 3.9 to run

     Once you have supported Python version installed you can start mindsdb as follows:

     1. create and activate venv:
     python3 -m venv venv
     source venv/bin/activate

     2. install MindsDB:
     pip3 install mindsdb

     3. Run MindsDB
     python3 -m mindsdb

     More instructions in https://docs.mindsdb.com
         """)
        exit(1)

    # --- VERSION MODE ----
    if args is not None and args.version:
        print(f'MindsDB {mindsdb_version}')
        sys.exit(0)

    # --- MODULE OR LIBRARY IMPORT MODE ----
    if args is not None and args.config is not None:
        config_path = args.config
        with open(config_path, 'r') as fp:
            user_config = json.load(fp)
    else:
        user_config = {}
        config_path = 'absent'
    os.environ['MINDSDB_CONFIG_PATH'] = config_path

    config = Config()
    create_dirs_recursive(config['paths'])

    if telemetry_file_exists(config['storage_dir']):
        os.environ['CHECK_FOR_UPDATES'] = '0'
        logger.info('\n x telemetry disabled! \n')
    elif os.getenv('CHECK_FOR_UPDATES', '1').lower() in ['0', 'false', 'False'] or config.get('cloud', False):
        disable_telemetry(config['storage_dir'])
        logger.info('\n x telemetry disabled! \n')
    else:
        logger.info("✓ telemetry enabled")

    if os.environ.get("FLASK_SECRET_KEY") is None:
        os.environ["FLASK_SECRET_KEY"] = secrets.token_hex(32)

    # -------------------------------------------------------

    # initialization
    db.init()

    mp.freeze_support()
    config = Config()

    environment = config.get("environment")
    if environment == "aws_marketplace":
        try:
            register_oauth_client()
        except Exception as e:
            logger.error(f"Something went wrong during client register: {e}")
    elif environment != "local":
        try:
            aws_meta_data = get_aws_meta_data()
            config.update({
                'aws_meta_data': aws_meta_data
            })
        except Exception:
            pass

    is_cloud = config.get("cloud", False)

    if not is_cloud:
        logger.debug("Applying database migrations")
        try:
            from mindsdb.migrations import migrate
            migrate.migrate_to_head()
        except Exception as e:
            logger.error(f"Error! Something went wrong during DB migrations: {e}")

    if args.verbose is True:
        # Figure this one out later
        pass

    if args.install_handlers is not None:
        handlers_list = [s.strip() for s in args.install_handlers.split(",")]
        # import_meta = handler_meta.get('import', {})
        for handler_name, handler_meta in integration_controller.get_handlers_import_status().items():
            if handler_name not in handlers_list:
                continue
            import_meta = handler_meta.get("import", {})
            if import_meta.get("success") is True:
                logger.info(f"{'{0: <18}'.format(handler_name)} - already installed")
                continue
            result = install_dependencies(import_meta.get("dependencies", []))
            if result.get("success") is True:
                logger.info(
                    f"{'{0: <18}'.format(handler_name)} - successfully installed"
                )
            else:
                logger.info(
                    f"{'{0: <18}'.format(handler_name)} - error during dependencies installation: {result.get('error_message', 'unknown error')}"
                )
        sys.exit(0)

    logger.info(f"Version: {mindsdb_version}")
    logger.info(f"Configuration file: {config.config_path}")
    logger.info(f"Storage path: {config['paths']['root']}")
    logger.debug(f"User config: {user_config}")

#     TODO keep it?
#     for (
#         handler_name,
#         handler_meta,
#     ) in integration_controller.get_handlers_import_status().items():
#         import_meta = handler_meta.get("import", {})
#         if import_meta.get("success", False) is not True:
#             logger.info(
#                 """Some handlers cannot be imported. You can check list of available handlers by execute command in sql editor:
# select * from information_schema.handlers;"""
#             )
#             break

    if not is_cloud:
        # region creating permanent integrations
        for (
            integration_name,
            handler,
        ) in integration_controller.get_handlers_metadata().items():
            if handler.get("permanent"):
                integration_meta = integration_controller.get(name=integration_name)
                if integration_meta is None:
                    integration_record = db.Integration(
                        name=integration_name,
                        data={},
                        engine=integration_name,
                        company_id=None,
                    )
                    db.session.add(integration_record)
                    db.session.commit()
        # endregion

        # region Mark old predictors as outdated
        is_modified = False
        predictor_records = (
            db.session.query(db.Predictor)
            .filter(db.Predictor.deleted_at.is_(None))
            .all()
        )
        if len(predictor_records) > 0:
            (
                sucess,
                compatible_versions,
            ) = get_versions_where_predictors_become_obsolete()
            if sucess is True:
                compatible_versions = [version.parse(x) for x in compatible_versions]
                mindsdb_version_parsed = version.parse(mindsdb_version)
                compatible_versions = [x for x in compatible_versions if x <= mindsdb_version_parsed]
                if len(compatible_versions) > 0:
                    last_compatible_version = compatible_versions[-1]
                    for predictor_record in predictor_records:
                        if (
                            isinstance(predictor_record.mindsdb_version, str)
                            and version.parse(predictor_record.mindsdb_version) < last_compatible_version
                        ):
                            predictor_record.update_status = "available"
                            is_modified = True
        if is_modified is True:
            db.session.commit()
        # endregion

    if args.api is None:  # If "--api" option is not specified, start the default APIs
        api_arr = ['http', 'mysql']
    elif args.api == "":  # If "--api=" (blank) is specified, don't start any APIs
        api_arr = []
    else:  # The user has provided a list of APIs to start
        api_arr = args.api.split(',')

    apis = {
        api: {
            'port': config['api'][api]['port'],
            'process': None,
            'started': False
        } for api in api_arr
    }

    start_functions = {
        'http': start_http,
        'mysql': start_mysql,
        'mongodb': start_mongo,
        'postgres': start_postgres,
        'jobs': start_scheduler,
        'tasks': start_tasks,
        'ml_task_queue': start_ml_task_queue
    }

    if config.get("jobs", {}).get("disable") is not True:
        apis["jobs"] = {"process": None, "started": False}

    # disabled on cloud
    if config.get('tasks', {}).get('disable') is not True:
        apis['tasks'] = {
            'process': None,
            'started': False
        }

    if args.ml_task_queue_consumer is True:
        apis['ml_task_queue'] = {
            'process': None,
            'started': False
        }

    # TODO this 'ctx' is eclipsing 'context' class imported as 'ctx'
    ctx = mp.get_context("spawn")
    for api_name, api_data in apis.items():
        if api_data["started"]:
            continue
        logger.info(f"{api_name} API: starting...")
        try:
            process_args = (args.verbose,)
            if api_name == 'http':
                process_args = (args.verbose, args.no_studio)
            p = ctx.Process(target=start_functions[api_name], args=process_args, name=api_name)
            p.start()
            api_data["process"] = p
        except Exception as e:
            logger.error(
                f"Failed to start {api_name} API with exception {e}\n{traceback.format_exc()}"
            )
            close_api_gracefully(apis)
            raise e

    atexit.register(close_api_gracefully, apis=apis)

    async def wait_api_start(api_name, pid, port):
        timeout = 60
        start_time = time.time()
        started = is_pid_listen_port(pid, port)
        while (time.time() - start_time) < timeout and started is False:
            await asyncio.sleep(0.5)
            started = is_pid_listen_port(pid, port)
        return api_name, port, started

    async def wait_apis_start():
        futures = [
            wait_api_start(api_name, api_data["process"].pid, api_data["port"])
            for api_name, api_data in apis.items()
            if "port" in api_data
        ]
        for i, future in enumerate(asyncio.as_completed(futures)):
            api_name, port, started = await future
            if started:
                logger.info(f"{api_name} API: started on {port}")
            else:
                logger.error(f"ERROR: {api_name} API cant start on {port}")

    async def join_process(process, name):
        try:
            while process.is_alive():
                process.join(1)
                await asyncio.sleep(0)
        except KeyboardInterrupt:
            logger.info("Got keyboard interrupt, stopping APIs")
            close_api_gracefully(apis)
        finally:
            logger.info(f"{name} API: stopped")

    async def gather_apis():
        await asyncio.gather(
            *[join_process(api_data['process'], api_name) for api_name, api_data in apis.items()],
            return_exceptions=False
        )

    ioloop = asyncio.new_event_loop()
    ioloop.run_until_complete(wait_apis_start())

    threading.Thread(target=do_clean_process_marks).start()

    ioloop.run_until_complete(gather_apis())
    ioloop.close()
