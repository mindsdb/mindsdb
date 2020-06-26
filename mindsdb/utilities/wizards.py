import os
import json


def _in(ask, default, use_default):
    if use_default:
        return default

    user_input = input(f'{ask} (Default: {default})')
    if user_input is None or user_input == '':
        user_input = default
    return user_input

def auto_config(python_path,pip_path,predictor_dir,datasource_dir):
    config = {
        "debug": False
        ,"config_version": 1
        ,"python_interpreter": python_path
        ,"pip_path": pip_path
        ,"api": {
        }
        ,"integrations": {
          "clickhouse": {
              "enabled": False
          }
          ,"mariadb": {
              "enabled": False
          }
        }
        ,"interface":{
          "mindsdb_native": {
              "enabled": True
              ,"storage_dir": predictor_dir
          }
          ,"lightwood": {
               "enabled": True
          }
          ,"datastore": {
               "enabled": True
               ,"storage_dir": datasource_dir
          }
          ,"dataskillet": {
               "enabled": False
          }
        }
    }

    return config

def cli_config(python_path,pip_path,predictor_dir,datasource_dir,config_dir,use_default=False):
    config = auto_config(python_path,pip_path,predictor_dir,datasource_dir)

    http = _in('Enable HTTP API ? [Y/N]','Y',use_default)
    if http in ['Y','y']:
        config['api']['http'] = {}
        config['api']['http']['host'] = _in('HTTP interface host: ','0.0.0.0',use_default)
        config['api']['http']['port'] = _in('HTTP interface port: ','47334',use_default)

    mysql = _in('Enable MYSQL API ? [Y/N]','Y',use_default)
    if mysql in ['Y','y']:
        config['api']['mysql'] = {
            "certificate_path": "cert.pem"
            ,"log": {
                "format": "%(asctime)s - %(levelname)s - %(message)s",
                "folder": "logs/",
                "file": "mysql.log",
                "file_level": "INFO",
                "console_level": "INFO"
            }
            ,"datasources": []
        }
        config['api']['mysql']['host'] = _in('MYSQL interface host','127.0.0.1',use_default)
        config['api']['mysql']['port'] = _in('MYSQL interface port','47335',use_default)
        config['api']['mysql']['user'] = _in('MYSQL interface user','mindsdb',use_default)
        config['api']['mysql']['password'] = _in('MYSQL interface password','',use_default)

    clickhouse = _in('Connect to clickhouse ? [Y/N]','Y',use_default)
    if clickhouse in ['Y','y']:
        config['integrations']['clickhouse']['enabled'] = True
        config['integrations']['clickhouse']['host'] = _in('Clickhouse host: ','localhost',use_default)
        config['integrations']['clickhouse']['port'] = _in('Clickhouse port: ','8123',use_default)
        config['integrations']['clickhouse']['user'] = _in('Clickhouse user: ','default',use_default)
        config['integrations']['clickhouse']['password'] = _in('Clickhouse password: ','',use_default)

    mariadb = _in('Connect to Mariadb ? [Y/N]','Y',use_default)
    if clickhouse in ['Y','y']:
        config['integrations']['mariadb']['enabled'] = True
        config['integrations']['mariadb']['host'] = _in('Mariadb host: ','localhost',use_default)
        config['integrations']['mariadb']['port'] = _in('Mariadb port: ','3306',use_default)
        config['integrations']['mariadb']['user'] = _in('Mariadb user: ','root',use_default)
        config['integrations']['mariadb']['password'] = _in('Mariadb password: ','',use_default)

    config_path = os.path.join(config_dir,'config.json')
    with open(config_path, 'w') as fp:
        json.dump(config, fp, indent=4, sort_keys=True)

    return config_path


def daemon_creator(python_path, config_path=None):
    daemon_path = '/etc/systemd/system/mindsdb.service'
    service_txt = f"""[Unit]
Description=Mindsdb

[Service]
ExecStart={python_path} -m mindsdb_server { "--config="+config_path  if config_path else ""}

[Install]
WantedBy=multi-user.target
""".strip(' ')

    try:
        with open(daemon_path, 'w') as fp:
            fp.write(service_txt)
    except Exception as e:
        print(f'Failed to create daemon, error: {e}')

    try:
        os.system('systemctl daemon-reload')
    except Exception as e:
        print(f'Failed to load daemon, error: {e}')
    return daemon_path


def make_executable(python_path, exec_path, config_path=None):
    text = f"""#!/bin/bash
{python_path} -m mindsdb_server { "--config="+config_path  if config_path else ""}
"""

    with open(exec_path, 'w') as fp:
        fp.write(text)

    os.system(f'chmod +x {exec_path}')
