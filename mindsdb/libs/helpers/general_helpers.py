import platform
import re
import urllib
import uuid
from pathlib import Path
import pickle
import requests
from contextlib import contextmanager
import ctypes
import io
import os, sys
import tempfile


from mindsdb.__about__ import __version__
from mindsdb.config import CONFIG
from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.constants.mindsdb import *


def get_key_for_val(key, dict_map):
    for orig_col in dict_map:
        if dict_map[orig_col] == key:
            return orig_col

    return key

def convert_cammelcase_to_snake_string(cammel_string):
    """
    Converts snake string to cammelcase

    :param cammel_string: as described
    :return: the snake string AsSaid -> as_said
    """

    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', cammel_string)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def get_label_index_for_value(value, labels):
    """
    This will define what index interval does a value belong to given a list of labels

    :param value: value to evaluate
    :param labels: the labels to compare against
    :return: an index value
    """
    if  value is None:
        return 0 # Todo: Add another index for None values

    if type(value) == type(''):
        try:
            index = labels.index(value)
        except:
            index = 0
    else:
        for index, compare_to in enumerate(labels):
            if value < compare_to:
                index = index
                break

    return index

def convert_snake_to_cammelcase_string(snake_str, first_lower = False):
    """
    Converts snake to cammelcase

    :param snake_str: as described
    :param first_lower: if you want to start the string with lowercase as_said -> asSaid
    :return: cammelcase string
    """

    components = snake_str.split('_')

    if first_lower == True:
        # We capitalize the first letter of each component except the first one
        # with the 'title' method and join them together.
        return components[0] + ''.join(x.title() for x in components[1:])
    else:
        return ''.join(x.title() for x in components)

def check_for_updates():
    """
    Check for updates of mindsdb
    it will ask the mindsdb server if there are new versions, if there are it will log a message

    :return: None
    """

    # tmp files
    uuid_file = CONFIG.MINDSDB_STORAGE_PATH + '/../uuid.mdb_base'
    mdb_file = CONFIG.MINDSDB_STORAGE_PATH + '/start.mdb_base'

    uuid_file_path = Path(uuid_file)
    if uuid_file_path.is_file():
        uuid_str = open(uuid_file).read()
    else:
        uuid_str = str(uuid.uuid4())
        try:
            open(uuid_file, 'w').write(uuid_str)
        except:
            log.warning('Cannot store token, Please add write permissions to file:' + uuid_file)
            uuid_str = uuid_str + '.NO_WRITE'

    file_path = Path(mdb_file)
    if file_path.is_file():
        token = open(mdb_file).read()
    else:
        token = '{system}|{version}|{uid}'.format(system=platform.system(), version=__version__, uid=uuid_str)
        try:
            open(mdb_file,'w').write(token)
        except:
            log.warning('Cannot store token, Please add write permissions to file:'+mdb_file)
            token = token+'.NO_WRITE'
    extra = urllib.parse.quote_plus(token)
    try:
        r = requests.get('http://mindsdb.com/updates/check/{extra}'.format(extra=extra), headers={'referer': 'http://check.mindsdb.com/?token={token}'.format(token=token)})
    except:
        log.warning('Could not check for updates')
        return
    try:
        # TODO: Extract version, compare with version in version.py
        ret = r.json()

        if 'version' in ret and ret['version']!= __version__:
            pass
            #log.warning("There is a new version of MindsDB {version}, please do:\n    pip3 uninstall mindsdb\n    pip3 install mindsdb --user".format(version=ret['version']))
        else:
            log.debug('MindsDB is up to date!')

    except:
        log.warning('could not check for MindsDB updates')

def pickle_obj(object_to_pickle):
    """
    Returns a version of self that can be serialized into mongodb or tinydb
    :return: The data of an object serialized via pickle and decoded as a latin1 string
    """

    return pickle.dumps(object_to_pickle).decode(encoding='latin1')


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

    for i,ele in enumerate(arr):
        if value == None:
            return -1
        value = float(str(value).replace(',','.'))
        if ele > value:
            return i - 1

    return len(arr)-1


def get_value_bucket(value, buckets, col_stats):
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
    else:
        bucket = len(buckets) # for null values

    return bucket


def evaluate_accuracy(predictions, real_values, col_stats, output_columns):
    score = 0
    for output_column in output_columns:
        cummulative_scores = 0
        if 'percentage_buckets' in col_stats[output_column]:
            bucket = col_stats[output_column]['percentage_buckets']
        else:
            bucket = None

        for i in range(len(real_values[output_column])):
            pred_val_bucket = get_value_bucket(predictions[output_column][i], bucket, col_stats[output_column])
            if pred_val_bucket is None:
                if predictions[output_column][i] == real_values[output_column][i]:
                    cummulative_scores += 1
            elif pred_val_bucket == get_value_bucket(real_values[output_column][i], bucket, col_stats[output_column]):
                cummulative_scores += 1

        score += cummulative_scores/len(predictions[output_column])
    score = score/len(output_columns)
    if score == 0:
        score = 0.00000001
    return score


class suppress_stdout_stderr(object):
    def __init__(self):
        # Open a pair of null files
        self.null_fds =  [os.open(os.devnull,os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.c_stdout = sys.stdout.fileno() #int(ctypes.c_void_p.in_dll(ctypes.CDLL(None), 'stdout'))
        self.c_stderr = sys.stderr.fileno() #int(ctypes.c_void_p.in_dll(ctypes.CDLL(None), 'stderr'))

        self.save_fds = [os.dup(self.c_stdout), os.dup(self.c_stderr)]

    def __enter__(self):
        # Assign the null pointers to stdout and stderr.
        os.dup2(self.null_fds[0],self.c_stdout)
        os.dup2(self.null_fds[1],self.c_stderr)

    def __exit__(self, *_):
        # Re-assign the real stdout/stderr back to (1) and (2)
        os.dup2(self.save_fds[0],self.c_stdout)
        os.dup2(self.save_fds[1],self.c_stderr)
        # Close all file descriptors
        for fd in self.null_fds + self.save_fds:
            os.close(fd)

@contextmanager
# @TODO: Make it work with mindsdb logger/log levels... maybe
def disable_ludwig_output():
    try:
        try:
            old_tf_loglevel = os.environ['TF_CPP_MIN_LOG_LEVEL']
        except:
            old_tf_loglevel = '2'
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
        # Maybe get rid of this to not supress all errors and stdout
        with suppress_stdout_stderr():
            yield
    finally:
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = old_tf_loglevel
