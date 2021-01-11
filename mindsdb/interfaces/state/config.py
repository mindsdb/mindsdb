import inspect
from pathlib import Path
import os
import json
import hashlib
import datetime
from mindsdb.interfaces.state.schemas import Configuration, session
from mindsdb.utilities.fs import create_dirs_recursive
from mindsdb.utilities.fs import create_directory
from copy import deepcopy


default_config = {
    "log": {
        "level": {
            "console": "ERROR",
            "file": "WARNING"
        }

    },
    "debug": False,
    "integrations": {},
    "api": {
        "http": {
            "host": "127.0.0.1",
            "port": "47334"
        },
        "mysql": {
            "host": "127.0.0.1",
            "password": "",
            "port": "47335",
            "user": "mindsdb",
            "database": "mindsdb",
            "ssl": True
        },
        "mongodb": {
            "host": "127.0.0.1",
            "port": "47336",
            "database": "mindsdb"
        }
    }
    ,"company_id": None
    ,"paths": {

    }
    ,"permanent_storage": {
        "location": "local"
        '''
        e.g:
        "credentials": {
                    aws_access_key_id
                    aws_secret_access_key
                    aws_session_token
        }
        '''
    }
}

def _get_paths():
    this_file_path = os.path.abspath(inspect.getfile(inspect.currentframe()))
    mindsdb_path = os.path.abspath(Path(this_file_path).parent.parent.parent.parent)

    return [f'{mindsdb_path}/var/']

    # if windows
    if os.name == 'nt':
        return [os.path.join(os.environ['APPDATA'], 'mindsdb')]
    else:
        retrun [
            '/var/lib/mindsdb'
            ,'{}/.local/var/lib/mindsdb'.format(Path.home())
        ]

    return tuples

def _get_or_create_dir_struct():
    for _dir in _get_paths():
        try:
            assert os.path.exists(_dir)
            assert os.access(_dir, os.W_OK) is True
            return _dir
        except Exception:
            pass

    for _dir in _get_paths():
        try:
            create_directory(_dir)
            assert os.access(_dir, os.W_OK) is True
            return _dir
        except Exception:
            pass

    raise Exception('MindsDB storage directory does not exist and could not be created')

def _null_to_empty(config):
    ''' changing user input to formalised view
    '''
    for integration in config.get('integrations', {}).values():
        password = integration.get('password')
        password = '' if password is None else str(password)
        integration['password'] = str(password)

    password = config['api']['mysql'].get('password')
    password = '' if password is None else str(password)
    config['api']['mysql']['password'] = str(password)
    return config

def _merge_key_recursive(target_dict, source_dict, key):
    if key not in target_dict:
        target_dict[key] = source_dict[key]
    elif not isinstance(target_dict[key], dict) or not isinstance(source_dict[key], dict):
        target_dict[key] = source_dict[key]
    else:
        for k in list(source_dict[key].keys()):
            _merge_key_recursive(target_dict[key], source_dict[key], k)

def _merge_configs(config, other_config):
    for key in list(other_config.keys()):
        _merge_key_recursive(config, other_config, key)
    return config

class Config(object):
    _config = None
    _no_db = None

    def __init__(self, config_path, no_db=False):
        self.no_db = no_db
        self.last_updated = datetime.datetime.now() - datetime.timedelta(hours=1)

        if isinstance(config_path, Config):
            config_path = config_path.as_dict()

        if isinstance(config_path, dict):
            config = config_path
        else:
            try:
                with open(config_path, 'r') as fp:
                    config = json.load(fp)
            except:
                config = {}

        config = deepcopy(config)

        self._read(config.get('company_id', None))

        if self._config is not None:
            config = _merge_configs(self._config, config)

        storage_dir = config.get('storage_dir', _get_or_create_dir_struct())
        if os.path.isabs(storage_dir) is False:
            storage_dir = os.path.normpath(storage_dir)
        config['storage_dir'] = storage_dir

        config = _merge_configs(default_config, config)
        config = _null_to_empty(config)

        config['paths']['root'] = storage_dir
        config['paths']['datasources'] = os.path.join(storage_dir, 'datasources')
        config['paths']['predictors'] = os.path.join(storage_dir, 'predictors')
        config['paths']['static'] = os.path.join(storage_dir, 'static')
        config['paths']['tmp'] = os.path.join(storage_dir, 'tmp')
        config['paths']['log'] = os.path.join(storage_dir, 'log')
        config['paths']['obsolete'] = {
            'predictors': os.path.join(storage_dir, 'obsolete', 'predictors'),
            'datasources': os.path.join(storage_dir, 'obsolete', 'datasources')
        }

        create_dirs_recursive(config['paths'])
        self._config = config

        self._save()

    @property
    def paths(self):
        return self._config['paths']

    def _read(self, company_id=None):
        try:
            if company_id is None:
                company_id = self._config['company_id']
        except Exception as e:
            company_id = None

        if Configuration.query.filter(Configuration.company_id == company_id).filter(Configuration.modified_at > self.last_updated).first() is None:
            return

        if self.no_db:
            return

        try:
            self._config = json.loads(Configuration.query.filter_by(company_id=company_id).first().data)
            self.last_updated = datetime.datetime.now()
        except Exception as e:
            self._config = None

    def _save(self):
        if self.no_db:
            return
        try:
            config_record = Configuration.query.filter_by(company_id=self._config['company_id']).first()
            config_record.data = json.dumps(self._config)
        except Exception as e:
            config_record = Configuration(company_id=self._config['company_id'],data=json.dumps(self._config))
            session.add(config_record)

        session.commit()

    def __getitem__(self, key):
        self._read()
        return self._config[key]

    def get(self, key, default=None):
        self._read()
        return self._config.get(key, default)

    def as_dict(self):
        self._read()
        return self._config

    def set(self, key_chain, value, delete=False):
        # @TOOD Maybe add a mutex here ? But that seems a bit overkill to be honest
        value = deepcopy(value)
        self._read()
        c = self._config
        for i, k in enumerate(key_chain):
            if k in c and i + 1 < len(key_chain):
                c = c[k]
            elif k not in c and i + 1 < len(key_chain):
                c[k] = {}
                c = c[k]
            else:
                if delete:
                    del c[k]
                else:
                    c[k] = value
        self._save()
        self._read()

    # Higher level interface
    def add_db_integration(self, name, dict):
        dict = deepcopy(dict)
        dict['date_last_update'] = str(datetime.datetime.now()).split('.')[0]
        if 'database_name' not in dict:
            dict['database_name'] = name
        if 'publish' not in dict:
            dict['publish'] = True

        self.set(['integrations', name], dict)

    def modify_db_integration(self, name, dict):
        dict = deepcopy(dict)
        old_dict = self._config['integrations'][name]
        for k in old_dict:
            if k not in dict:
                dict[k] = old_dict[k]

        self.add_db_integration(name, dict)

    def remove_db_integration(self, name):
        self.set(['integrations', name], None, True)
