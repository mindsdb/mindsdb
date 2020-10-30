import inspect
import os
from pathlib import Path
import json
import shutil
import pickle
from distutils.version import LooseVersion
import logging


def create_directory(path):
    path = Path(path)
    path.mkdir(mode=0o777, exist_ok=True, parents=True)


def get_paths():
    this_file_path = os.path.abspath(inspect.getfile(inspect.currentframe()))
    mindsdb_path = os.path.abspath(Path(this_file_path).parent.parent.parent)

    tuples = [
        (
            f'{mindsdb_path}/etc/',
            f'{mindsdb_path}/var/'
        )
    ]

    # if windows
    if os.name == 'nt':
        tuples.extend([
            (
                os.path.join(os.environ['APPDATA'], 'mindsdb'),
                os.path.join(os.environ['APPDATA'], 'mindsdb'),
            )
        ])
    else:
        tuples.extend([
            (
                '/etc/mindsdb',
                '/var/lib/mindsdb'
            ),
            (
                '~/.local/etc/mindsdb',
                '~/.local/var/lib/mindsdb'
            )
        ])

    return tuples


def get_or_create_dir_struct():
    for tup in get_paths():
        try:
            for dir in tup:
                assert(os.path.exists(dir))
                assert(os.access(dir, os.W_OK) is True)

            config_dir = tup[0]
            if 'DEV_CONFIG_PATH' in os.environ:
                config_dir = os.environ['DEV_CONFIG_PATH']

            return config_dir, tup[1]
        except Exception:
            pass

    for tup in get_paths():
        try:
            for dir in tup:
                create_directory(dir)
                assert(os.access(dir, os.W_OK) is True)

            config_dir = tup[0]
            if 'DEV_CONFIG_PATH' in os.environ:
                config_dir = os.environ['DEV_CONFIG_PATH']

            return config_dir, tup[1]

        except Exception:
            pass

    raise Exception('MindsDB storage directory does not exist and could not be created')


def do_init_migration(paths):
    ''' That initial migration for storage structure. Should be called once after user updates to 2.8.0.
        When we decide all active users has update (after a month?), this function can be removed.
    '''
    # move predictors files by their directories
    endings = [
        '_heavy_model_metadata.pickle',
        '_light_model_metadata.pickle',
        '_lightwood_data'
    ]
    for ending in endings:
        for p in Path(paths['predictors']).iterdir():
            if p.is_file() and p.name.endswith(ending):
                predictor_name = p.name[:-len(ending)]
                predictor_path = Path(paths['predictors']).joinpath(predictor_name)
                create_directory(predictor_path)
                new_file_name = ending[1:]
                shutil.move(
                    str(p),
                    str(predictor_path.joinpath(new_file_name))
                )
                if new_file_name == 'light_model_metadata.pickle':
                    with open(str(predictor_path.joinpath(new_file_name)), 'rb') as fp:
                        lmd = pickle.load(fp)

                    if 'ludwig_data' in lmd and 'ludwig_save_path' in lmd['ludwig_data']:
                        lmd['ludwig_data']['ludwig_save_path'] = os.path.join(paths['predictors'], lmd['name'], 'ludwig_data')

                    if 'lightwood_data' in lmd and 'save_path' in lmd['lightwood_data']:
                        lmd['lightwood_data']['save_path'] = os.path.join(paths['predictors'], lmd['name'], 'lightwood_data')

                    with open(os.path.join(paths['predictors'], lmd['name'], 'light_model_metadata.pickle'), 'wb') as fp:
                        pickle.dump(lmd, fp, protocol=pickle.HIGHEST_PROTOCOL)

    for p in Path(paths['predictors']).iterdir():
        if p.is_file() and p.name != 'start.mdb_base':
            p.unlink()

    # mopve each datasource files from ds_name/datasource/{file} to ds_name/{file}
    for p in Path(paths['datasources']).iterdir():
        if p.is_dir():
            datasource_folder = p.joinpath('datasource')
            if datasource_folder.is_dir():
                for f in datasource_folder.iterdir():
                    shutil.move(
                        str(f),
                        str(p.joinpath(f.name))
                    )
                shutil.rmtree(datasource_folder)


def update_versions_file(config, versions):
    versions_file_path = os.path.join(config.paths['root'], 'versions.json')
    old_versions = {}
    if Path(versions_file_path).is_file():
        try:
            with open(versions_file_path, 'rt') as f:
                old_versions = json.loads(f.read())
        except Exception:
            pass

    # do here anything for update
    if len(old_versions) == 0:
        do_init_migration(config.paths)

    with open(versions_file_path, 'wt') as f:
        json.dump(versions, f, indent=4, sort_keys=True)


def create_dirs_recursive(path):
    if isinstance(path, dict):
        for p in path.values():
            create_dirs_recursive(p)
    elif isinstance(path, str):
        create_directory(path)
    else:
        raise ValueError(f'Wrong path: {path}')


def archive_obsolete_predictors(config, old_version):
    ''' move all predictors trained on mindsdb with version less than
        old_version to folder for obsolete predictors

        Predictors are outdated in:
        v2.11.0 - in mindsdb_native added ['data_analysis_v2']['columns']
    '''
    obsolete_predictors = []
    obsolete_predictors_dir = config.paths['obsolete']['predictors']
    for f in Path(config.paths['predictors']).iterdir():
        if f.is_dir():
            if not f.joinpath('versions.json').is_file():
                obsolete_predictors.append(f.name)
            else:
                with open(f.joinpath('versions.json'), 'rt') as vf:
                    versions = json.loads(vf.read())
                if LooseVersion(versions['mindsdb']) < LooseVersion(old_version):
                    obsolete_predictors.append(f.name)
    if len(obsolete_predictors) > 0:
        print('These predictors are outdated and moved to {storage_dir}/obsolete/ folder:')
        for p in obsolete_predictors:
            print(f' - {p}')
            new_path = Path(obsolete_predictors_dir).joinpath(p)
            if Path(obsolete_predictors_dir).joinpath(p).is_dir():
                i = 1
                while Path(obsolete_predictors_dir).joinpath(f'{p}_{i}').is_dir():
                    i += 1
                new_path = Path(obsolete_predictors_dir).joinpath(f'{p}_{i}')

            shutil.move(
                Path(config.paths['predictors']).joinpath(p),
                new_path
            )


def remove_corrupted_predictors(config, mindsdb_native):
    ''' Checking that all predictors can be loaded.
        If not - then move such predictir to {storage_dir}/tmp/corrupted_predictors
    '''
    for p in [x for x in Path(config.paths['predictors']).iterdir() if x.is_dir()]:
        model_name = p.name
        try:
            mindsdb_native.get_model_data(model_name)
        except Exception as e:
            log = logging.getLogger('mindsdb.main')
            log.error(f"Error: predictor '{model_name}' corrupted. Move predictor data to '{{storage_dir}}/tmp/corrupted_predictors' dir.")
            log.error(f"Reason is: {e}")
            corrupted_predictors_dir = Path(config.paths['tmp']).joinpath('corrupted_predictors')
            create_directory(corrupted_predictors_dir)
            shutil.move(
                str(p),
                str(corrupted_predictors_dir.joinpath(model_name))
            )
