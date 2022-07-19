import atexit
import traceback
import sys
import os
import time
import asyncio
import signal
import psutil
import json
import shutil

import torch.multiprocessing as mp
mp.set_start_method('spawn')
from packaging import version

from mindsdb.api.http.start import start as start_http
from mindsdb.api.mysql.start import start as start_mysql
from mindsdb.api.mongo.start import start as start_mongo
from mindsdb.utilities.config import Config
from mindsdb.utilities.ps import is_pid_listen_port, get_child_pids
from mindsdb.utilities.functions import args_parse, get_versions_where_predictors_become_obsolete
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.utilities.log import log
from mindsdb.interfaces.stream.stream import StreamController
from mindsdb.interfaces.stream.utilities import STOP_THREADS_EVENT
from mindsdb.interfaces.model.model_interface import ray_based, ModelInterface
from mindsdb.interfaces.database.integrations import IntegrationController
import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.utilities.install import install_dependencies


COMPANY_ID = os.environ.get('MINDSDB_COMPANY_ID', None)


def close_api_gracefully(apis):
    try:
        for api in apis.values():
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
        if ray_based:
            os.system('ray stop --force')
    except KeyboardInterrupt:
        sys.exit(0)
    except psutil.NoSuchProcess:
        pass


if __name__ == '__main__':
    mp.freeze_support()
    args = args_parse()
    config = Config()

    is_cloud = config.get('cloud', False)

    if not is_cloud:
        print('Applying database migrations:')
        try:
            from mindsdb.migrations import migrate
            migrate.migrate_to_head()
        except Exception as e:
            print(f'Error! Something went wrong during DB migrations: {e}')

    if args.verbose is True:
        # Figure this one out later
        pass

    integration_controller = WithKWArgsWrapper(IntegrationController(), company_id=COMPANY_ID)
    if args.install_handlers is not None:
        handlers_list = [s.strip() for s in args.install_handlers.split(',')]
        # import_meta = handler_meta.get('import', {})
        for handler_name, handler_meta in integration_controller.get_handlers_import_status().items():
            if handler_name not in handlers_list:
                continue
            import_meta = handler_meta.get('import', {})
            if import_meta.get('success') is True:
                print(f"{'{0: <18}'.format(handler_name)} - already installed")
                continue
            result = install_dependencies(import_meta.get('dependencies', []))
            if result.get('success') is True:
                print(f"{'{0: <18}'.format(handler_name)} - successfully installed")
            else:
                print(f"{'{0: <18}'.format(handler_name)} - error during dependencies installation: {result.get('error_message', 'unknown error')}")
        sys.exit(0)

    os.environ['DEFAULT_LOG_LEVEL'] = config['log']['level']['console']
    os.environ['LIGHTWOOD_LOG_LEVEL'] = config['log']['level']['console']

    # Switch to this once the native interface has it's own thread :/
    ctx = mp.get_context('spawn')

    from mindsdb.__about__ import __version__ as mindsdb_version
    print(f'Version {mindsdb_version}')

    print(f'Configuration file:\n   {config.config_path}')
    print(f"Storage path:\n   {config['paths']['root']}")

    # @TODO Backwards compatibility for tests, remove later
    model_interface = WithKWArgsWrapper(ModelInterface(), company_id=COMPANY_ID)
    integration_controller = WithKWArgsWrapper(IntegrationController(), company_id=COMPANY_ID)
    for handler_name, handler_meta in integration_controller.get_handlers_import_status().items():
        import_meta = handler_meta.get('import', {})
        if import_meta.get('success', False) is not True:
            print(f"Can't import handler '{handler_name}': {import_meta.get('error_message', 'unknown error')}")

    raw_model_data_arr = model_interface.get_models()
    model_data_arr = []
    for model in raw_model_data_arr:
        if model['status'] == 'complete':
            x = model_interface.get_model_data(model['name'])
            try:
                model_data_arr.append(model_interface.get_model_data(model['name']))
            except Exception:
                pass

    if not is_cloud:
        # region creating permanent integrations
        for integration_name in ['files', 'views']:
            integration_meta = integration_controller.get(name=integration_name)
            if integration_meta is None:
                integration_record = db.Integration(
                    name=integration_name,
                    data={},
                    engine=integration_name,
                    company_id=None
                )
                db.session.add(integration_record)
                db.session.commit()
        # endregion

        # region Mark old predictors as outdated
        is_modified = False
        predictor_records = db.session.query(db.Predictor).all()
        if len(predictor_records) > 0:
            sucess, compatible_versions = get_versions_where_predictors_become_obsolete()
            if sucess is True:
                compatible_versions = [version.parse(x) for x in compatible_versions]
                mindsdb_version_parsed = version.parse(mindsdb_version)
                compatible_versions = [x for x in compatible_versions if x <= mindsdb_version_parsed]
                if len(compatible_versions) > 0:
                    last_compatible_version = compatible_versions[-1]
                    for predictor_record in predictor_records:
                        if (
                            isinstance(predictor_record.mindsdb_version, str) is not None
                            and version.parse(predictor_record.mindsdb_version) < last_compatible_version
                        ):
                            predictor_record.update_status = 'available'
                            is_modified = True
        if is_modified is True:
            db.session.commit()
        # endregion

        for integration_name in config.get('integrations', {}):
            try:
                it = integration_controller.get(integration_name)
                if it is not None:
                    integration_controller.delete(integration_name)
                print(f'Adding: {integration_name}')
                integration_data = config['integrations'][integration_name]
                engine = integration_data.get('type')
                if engine is not None:
                    del integration_data['type']
                integration_controller.add(integration_name, engine, integration_data)
            except Exception as e:
                log.error(f'\n\nError: {e} adding database integration {integration_name}\n\n')

        stream_controller = StreamController(COMPANY_ID)
        for integration_name, integration_meta in integration_controller.get_all(sensitive_info=True).items():
            if (
                integration_meta.get('type') in stream_controller.known_dbs
                and integration_meta.get('publish', False) is True
            ):
                print(f"Setting up stream: {integration_name}")
                stream_controller.setup(integration_name)
        del stream_controller

    del model_interface
    # @TODO Backwards compatibility for tests, remove later

    if args.api is None:
        api_arr = ['http', 'mysql']
    else:
        api_arr = args.api.split(',')

    with_nlp = False
    if 'nlp' in api_arr:
        with_nlp = True
        api_arr.remove('nlp')

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
        'mongodb': start_mongo
    }

    for api_name, api_data in apis.items():
        if api_data['started']:
            continue
        print(f'{api_name} API: starting...')
        try:
            if api_name == 'http':
                p = ctx.Process(target=start_functions[api_name], args=(args.verbose, args.no_studio, with_nlp))
            else:
                p = ctx.Process(target=start_functions[api_name], args=(args.verbose,))
            p.start()
            api_data['process'] = p
        except Exception as e:
            log.error(f'Failed to start {api_name} API with exception {e}\n{traceback.format_exc()}')
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
            wait_api_start(api_name, api_data['process'].pid, api_data['port'])
            for api_name, api_data in apis.items() if 'port' in api_data
        ]
        for i, future in enumerate(asyncio.as_completed(futures)):
            api_name, port, started = await future
            if started:
                print(f"{api_name} API: started on {port}")
            else:
                log.error(f"ERROR: {api_name} API cant start on {port}")

    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(wait_apis_start())
    ioloop.close()

    try:
        for api_data in apis.values():
            api_data['process'].join()
    except KeyboardInterrupt:
        print('Stopping stream integrations...')
        STOP_THREADS_EVENT.set()
        print('Closing app...')
