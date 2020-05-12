import platform
import re
import pickle
import urllib
import requests
from pathlib import Path
import uuid
from contextlib import contextmanager
import os, sys


from mindsdb.__about__ import __version__
from mindsdb.config import CONFIG
from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.constants.mindsdb import *
import imagehash
from PIL import Image


def check_for_updates():
    """
    Check for updates of mindsdb
    it will ask the mindsdb server if there are new versions, if there are it will log a message

    :return: None
    """

    # tmp files
    uuid_file = os.path.join(CONFIG.MINDSDB_STORAGE_PATH, '..', 'uuid.mdb_base')
    mdb_file = os.path.join(CONFIG.MINDSDB_STORAGE_PATH, 'start.mdb_base')

    if Path(uuid_file).is_file():
        uuid_str = open(uuid_file).read()
    else:
        uuid_str = str(uuid.uuid4())
        try:
            with open(uuid_file, 'w') as fp:
                fp.write(uuid_str)
        except:
            log.warning(f'Cannot store token, Please add write permissions to file: {uuid_file}')
            uuid_str = f'{uuid_str}.NO_WRITE'

    if Path(mdb_file).is_file():
        token = open(mdb_file, 'r').read()
    else:
        if CONFIG.IS_CI_TEST:
            uuid_str = 'travis'
        token = '{system}|{version}|{uid}'.format(system=platform.system(), version=__version__, uid=uuid_str)
        try:
            open(mdb_file,'w').write(token)
        except:
            log.warning(f'Cannot store token, Please add write permissions to file: {mdb_file}')
            token = f'{token}.NO_WRITE'

    try:
        ret = requests.get('https://public.api.mindsdb.com/updates/check/{token}'.format(token=token), headers={'referer': 'http://check.mindsdb.com/?token={token}'.format(token=token)})
        ret = ret.json()
    except Exception as e:
        try:
            log.warning(f'Got reponse: {ret} from update check server !')
        except:
            log.warning(f'Got no response from update check server !')
        log.warning(f'Could not check for updates, got excetpion {e} !')
        return

    try:
        if 'version' in ret and ret['version']!= __version__:
            log.warning("There is a new version of MindsDB {version}, please upgrade using:\npip3 uninstall mindsdb --upgrade".format(version=ret['version']))
        else:
            log.debug('MindsDB is up to date!')
    except:
        log.warning('could not check for MindsDB updates')


def convert_cammelcase_to_snake_string(cammel_string):
    """
    Converts snake string to cammelcase

    :param cammel_string: as described
    :return: the snake string AsSaid -> as_said
    """

    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', cammel_string)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def pickle_obj(object_to_pickle):
    """
    Returns a version of self that can be serialized into mongodb or tinydb
    :return: The data of an object serialized via pickle and decoded as a latin1 string
    """

    return pickle.dumps(object_to_pickle,protocol=pickle.HIGHEST_PROTOCOL).decode(encoding='latin1')


def unpickle_obj(pickle_string):
    """
    :param pickle_string: A latin1 encoded python str containing the pickle data
    :return: Returns an object generated from the pickle string
    """
    return pickle.loads(pickle_string.encode(encoding='latin1'))


def closest(arr, value):
    """
    :return: The index of the member of `arr` which is closest to `value`
    """

    if value == None:
        return -1

    for i,ele in enumerate(arr):
        value = float(str(value).replace(',', '.'))
        if ele > value:
            return i - 1

    return len(arr)-1


def get_value_bucket(value, buckets, col_stats, hmd=None):
    """
    :return: The bucket in the `histogram` in which our `value` falls
    """
    if buckets is None:
        return None

    if col_stats['data_subtype'] in (DATA_SUBTYPES.SINGLE, DATA_SUBTYPES.MULTIPLE):
        if value in buckets:
            bucket = buckets.index(value)
        else:
            bucket = len(buckets) # for null values

    elif col_stats['data_subtype'] in (DATA_SUBTYPES.BINARY, DATA_SUBTYPES.INT, DATA_SUBTYPES.FLOAT):
        bucket = closest(buckets, value)
    elif col_stats['data_subtype'] in (DATA_SUBTYPES.IMAGE) and hmd is not None:
        bucket = hmd['bucketing_algorithms'][col_name].predict(np.array(imagehash.phash(Image.open(value)).reshape(1, -1)))[0]
    else:
        bucket = len(buckets) # for null values

    return bucket


def evaluate_accuracy(predictions, full_dataset, col_stats, output_columns, hmd=None):
    score = 0
    for output_column in output_columns:
        cummulative_scores = 0
        if 'percentage_buckets' in col_stats[output_column]:
            buckets = col_stats[output_column]['percentage_buckets']
        else:
            buckets = None

        i = 0
        for real_value in full_dataset[output_column]:
            pred_val_bucket = get_value_bucket(predictions[output_column][i], buckets, col_stats[output_column], hmd)
            if pred_val_bucket is None:
                if predictions[output_column][i] == real_value:
                    cummulative_scores += 1
            elif pred_val_bucket == get_value_bucket(real_value, buckets, col_stats[output_column], hmd):
                cummulative_scores += 1
            i += 1

        score += cummulative_scores/len(predictions[output_column])
    score = score/len(output_columns)
    if score == 0:
        score = 0.00000001
    return score

class suppress_stdout_stderr(object):
    def __init__(self):
        try:
            crash = 'fileno' in dir(sys.stdout)
            # Open a pair of null files
            self.null_fds =  [os.open(os.devnull,os.O_RDWR) for x in range(2)]
            # Save the actual stdout (1) and stderr (2) file descriptors.
            self.c_stdout = sys.stdout.fileno()
            self.c_stderr = sys.stderr.fileno()

            self.save_fds = [os.dup(self.c_stdout), os.dup(self.c_stderr)]
        except:
            print('Can\'t disable output on Jupyter notebook')

    def __enter__(self):
        try:
            crash = 'dup2' in dir(os)
            # Assign the null pointers to stdout and stderr.
            os.dup2(self.null_fds[0],self.c_stdout)
            os.dup2(self.null_fds[1],self.c_stderr)
        except:
            print('Can\'t disable output on Jupyter notebook')

    def __exit__(self, *_):
        try:
            crash = 'dup2' in dir(os)
            # Re-assign the real stdout/stderr back to (1) and (2)
            os.dup2(self.save_fds[0],self.c_stdout)
            os.dup2(self.save_fds[1],self.c_stderr)
            # Close all file descriptors
            for fd in self.null_fds + self.save_fds:
                os.close(fd)
        except:
            print('Can\'t disable output on Jupyter notebook')

def get_tensorflow_colname(col):
    replace_chars = """ ,./;'[]!@#$%^&*()+{-=+~`}\\|:"<>?"""

    for char in replace_chars:
        col = col.replace(char,'_')
    col = re.sub('_+','_',col)

    return col

@contextmanager
def disable_console_output(activate=True):
    try:
        try:
            old_tf_loglevel = os.environ['TF_CPP_MIN_LOG_LEVEL']
        except:
            old_tf_loglevel = '2'
        # Maybe get rid of this to not supress all errors and stdout
        if activate:
            with suppress_stdout_stderr():
                yield
        else:
            yield
    finally:
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = old_tf_loglevel


def value_isnan(value):
    try:
        if isinstance(value, float):
            a = int(value)
        isnan = False
    except:
        isnan = True
    return isnan
