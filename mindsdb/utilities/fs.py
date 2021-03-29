import inspect
import os
from pathlib import Path
import tempfile


def create_directory(path):
    path = Path(path)
    path.mkdir(mode=0o777, exist_ok=True, parents=True)


def get_paths():
    this_file_path = os.path.abspath(inspect.getfile(inspect.currentframe()))
    mindsdb_path = os.path.abspath(Path(this_file_path).parent.parent)

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
                '{}/.local/etc/mindsdb'.format(Path.home()),
                '{}/.local/var/lib/mindsdb'.format(Path.home())
            )
        ])

    return tuples


def get_or_create_dir_struct():
    for tup in get_paths():
        try:
            for _dir in tup:
                assert os.path.exists(_dir)
                assert os.access(_dir, os.W_OK) is True

            config_dir = tup[0]

            return config_dir, tup[1]
        except Exception:
            pass

    for tup in get_paths():
        try:
            for _dir in tup:
                create_directory(_dir)
                assert os.access(_dir, os.W_OK) is True

            config_dir = tup[0]

            return config_dir, tup[1]

        except Exception:
            pass

    raise Exception('MindsDB storage directory does not exist and could not be created')


def create_dirs_recursive(path):
    if isinstance(path, dict):
        for p in path.values():
            create_dirs_recursive(p)
    elif isinstance(path, str):
        create_directory(path)
    else:
        raise ValueError(f'Wrong path: {path}')


def create_process_mark(folder='learn'):
    if os.name == 'posix':
        p = Path(tempfile.gettempdir()).joinpath(f'mindsdb/processes/{folder}/')
        p.mkdir(parents=True, exist_ok=True)
        p.joinpath(f'{os.getpid()}').touch()


def delete_process_mark(folder='learn'):
    if os.name == 'posix':
        p = Path(tempfile.gettempdir()).joinpath(f'mindsdb/processes/{folder}/').joinpath(f'{os.getpid()}')
        if p.exists():
            p.unlink()
