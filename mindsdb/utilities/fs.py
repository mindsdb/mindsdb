import inspect
import os
from pathlib import Path


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
                assert(os.access(dir, os.W_OK) == True)

            config_dir = tup[0]
            if 'DEV_CONFIG_PATH' in os.environ:
                config_dir = os.environ['DEV_CONFIG_PATH']

            return config_dir, tup[1]
        except Exception as e:
            pass

    for tup in get_paths():
        try:
            for dir in tup:
                create_directory(dir)
                assert(os.access(dir, os.W_OK) == True)

            config_dir = tup[0]
            if 'DEV_CONFIG_PATH' in os.environ:
                config_dir = os.environ['DEV_CONFIG_PATH']

            return config_dir, tup[1]

        except Exception as e:
            pass

    raise Exception(f'MindsDB storage directory: {path} does not exist and could not be created, trying another directory')
