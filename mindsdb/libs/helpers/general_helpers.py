import platform
import re
import urllib
import uuid
from pathlib import Path
import pickle

import requests

from mindsdb.__about__ import __version__
from mindsdb.config import CONFIG
from mindsdb.libs.data_types.mindsdb_logger import log


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
        token = '{system}|{version}|{uid}'.format(system=platform.system(), version=__version, uid=uuid_str)
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

def pickle(object):
    """
    Returns a version of self that can be serialized into mongodb or tinydb

    :return: The data of a ProbabilisticValidator serialized via pickle and decoded as a latin1 string
    """

    return pickle.dumps(object).decode(encoding='latin1')


def unpickle(pickle_string):
    """
    :param pickle_string: A latin1 encoded python str containing the pickle data
    :return: Returns a ProbabilisticValidator object generated from the pickle string
    """
    return pickle.loads(pickle_string.encode(encoding='latin1'))
