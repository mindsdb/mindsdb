import requests
import sys
import os
import shutil
import tarfile
import atexit


def cleanup(name):
    shutil.rmtree(f'{name}',ignore_errors=True)
    try:
        os.remove(f'{name}.tar.gz')
    except:
        pass

    shutil.rmtree(os.path.join('..', f'{name}'),ignore_errors=True)

    try:
        os.remove(os.path.join('..', f'{name}.tar.gz'))
    except:
        pass


def run_example(example_name):
    atexit.register(cleanup,name=example_name)

    with open(f'{example_name}.tar.gz', 'wb') as f:
        r = requests.get(f'https://mindsdb-example-data.s3.eu-west-2.amazonaws.com/{example_name}.tar.gz')
        f.write(r.content)

    tar = tarfile.open(f'{example_name}.tar.gz', 'r:gz')
    tar.extractall()
    tar.close()

    os.chdir(example_name)
    __import__(f'{example_name}.mindsdb_acc')


if __name__ == '__main__':
    run_example(sys.argv[1])
