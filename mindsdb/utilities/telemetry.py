import os
from pathlib import Path

TELEMETRY_FILE = 'telemetry.lock'


def enable_telemetry(storage_dir):
    os.environ['CHECK_FOR_UPDATES'] = '1'
    path = os.path.join(storage_dir, TELEMETRY_FILE)
    if os.path.exists(path):
        os.remove(path)


def disable_telemetry(storage_dir):
    os.environ['CHECK_FOR_UPDATES'] = '0'
    path = os.path.join(storage_dir, TELEMETRY_FILE)
    with open(path, 'w') as _:
        pass


def telemetry_file_exists(storage_dir):
    path = os.path.join(storage_dir, TELEMETRY_FILE)
    return os.path.exists(path)


def inject_telemetry_to_static(static_folder):
    TEXT = '<script>localStorage.isTestUser = true;</script>'
    index = Path(static_folder).joinpath('index.html')
    disable_telemetry = os.getenv('CHECK_FOR_UPDATES', '1').lower() in ['0', 'false', 'False']
    if index.is_file():
        with open(str(index), 'rt') as f:
            content = f.read()
        script_index = content.find('<script>')
        need_update = True
        if TEXT not in content and disable_telemetry:
            content = content[:script_index] + TEXT + content[script_index:]
        elif not disable_telemetry and TEXT in content:
            content = content.replace(TEXT, '')
        else:
            need_update = False

        if need_update:
            with open(str(index), 'wt') as f:
                f.write(content)
