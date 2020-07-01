import psutil
import time
import pathlib
import os
import json

def is_port_in_use(port_num):
    portsinuse = []
    conns = psutil.net_connections()
    portsinuse = [x.laddr[1] for x in conns if x.status == 'LISTEN']
    portsinuse.sort()
    return int(port_num) in portsinuse

def wait_port(port_num, timeout):
    start_time = time.time()

    in_use = is_port_in_use(port_num)
    while in_use is False and (time.time() - start_time) < timeout:
        time.sleep(2)
        in_use = is_port_in_use(port_num)

    return in_use

def prepare_config(config):
    for key in config._config['integrations'].keys():
        config._config['integrations'][key]['enabled'] = key == 'default_mariadb'

    TEMP_DIR = pathlib.Path(__file__).parent.absolute().joinpath('../temp/').resolve()

    config.merge({
        'interface': {
            'datastore': {
                'storage_dir': str(TEMP_DIR.joinpath('datastore/'))
            },
            'mindsdb_native': {
                'storage_dir': str(TEMP_DIR.joinpath('predictors/'))
            }
        }
    })

    if not os.path.isdir(config['interface']['datastore']['storage_dir']):
        os.makedirs(config['interface']['datastore']['storage_dir'])
    
    if not os.path.isdir(config['interface']['mindsdb_native']['storage_dir']):
        os.makedirs(config['interface']['mindsdb_native']['storage_dir'])

    temp_config_path = str(TEMP_DIR.joinpath('config.json').resolve())
    with open(temp_config_path, 'wt') as f:
        f.write(json.dumps(config._config))

    return temp_config_path
