import os
import json


def _in(ask, default, use_default):
    if use_default:
        return default

    user_input = input(f'{ask} (Default: {default})')
    if user_input is None or user_input == '':
        user_input = default

    if type(default) == int:
        user_input = int(user_input)

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
          "default_clickhouse": {
              "enabled": False
          }
          ,"default_mariadb": {
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

    crt_path = os.path.join(config_dir, 'cert.pem')
    with open(crt_path, 'w') as fp:
        fp.write("""-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDeItDNfMko2uLe
gVQUzPTLXx2hod9LKIN+S01TpwJvLi5185JZ8YhquwZnI19VvLQLyOIUMGeVxVvg
GPgkRA58liO4oyCjZGYJygwAN0gkMvJXeMXuNeezlfm3xiVvCk0+l6Vb8FqyJod/
P9sr6CyfQPolXSngZ8C7M/Fz3YWQ4O26L9in79eynCeJodXmzHsdrLHwKKOhv3F9
Chp5Rx82KRvoFrdhmUEGM+l4Aq1J1iUVEsAQHHiMWhJhfsiC1GjGErOjSfWKw9e0
ckbgqY/2ypXHdjsg0mI3eLO3E29SnG8OCWDLRcJVU1oHeX8hqk04c0urXhVQThN3
X6ecXD1dAgMBAAECggEAEVxjOUwhZKIGzSEKcz25fBOI+1LtYrBd5ob4GiuZUxsm
4m0Q6RqpcL4BOBpZnxfKcolWsgE+d0QfdBo/eoYfI7mQPSPyrxJvryAtY+7uInYg
3pk/zuhDnZOBGs3PqygA9X1gnRjh3b6JJHbXKE1S/3dSlYz8ct9o/riGjGmpwLLO
WuLbiRZoXRPCGWb1bIRpjVPn01YhlEvHyJsXktikm4pMUv+2QUZC7PU/eaAyY3eX
Y0qdgaxza8q7toFXENG2nI/4dL5T9d4Bg2642zIk+Ki43NbQox4BDeWaSBWQK+bB
DNDEjNnuGG0pTrdMD65TIOt7AoWeNCAqJZtSLDcoeQKBgQDwTHs1QX65Eo15cVh6
sClRYTP1d01t39cSdV7sX1Vtp/z7C3FeUlWpb8zgyo3wYJlo+7hyOAcY4KyBpTeS
aJGy1qIfD0qSt2ZNIvw1wfjiKa00ThMVW8urOcSMt+8+Q9SA/1nE/1iLNSc6M9Af
ixx34zxlg25vbEaYcFKqiGYNgwKBgQDspoXV4hiufqoPf7F5bYYrv5j3SXoaJZnM
RJngvBHohlSE9TwGvhHmy6xJAj1CRpoOOQpLoWgvxvWpdsCvcny3a8MY5AbyqOI4
banVDCW5jnRe5ak/ECoxP4uPK+5/9CUlW3cP+GKfRGU3H7OhadopvNfwjUPg+wB4
PXTCUvhznwKBgQCvKkFB//09Mb35QduKi7GCxgWXMKE7r8jahr5sNc5TQfqSkbPR
WtlgysOhNWYkTHZn5d59PERIKTb2xpXs3tcec4D4fTASJSiooBETqtMfIdxFXYhh
sGmV5mVVYps+Wzmj0wAAL1a/Gz7+GVjkNYbKCdYz9YviIx6O7ooED6u8uwKBgHuM
aJ0EYExhVpmm2doCQyT973dTBgs2jDfnrMp2hYb28pNDkOYYPzJWLQkkwSSjxXQd
dXGMv98JqWGi3O/7/n6oJQAOtE3lu80n+519rQhWBg0xK43/+3cgrNS/Y9GrfeUl
/l/5Fkv+IjWIOHjR0ZMuwzIUHlcL0+/ybc2yEYITAoGAc5shIP9wvjEGexemj1u1
mp2XwZ7zc0yyZA2icgsYAED6CVoNyrvU6KUm3m1fwEsHPdjk9vLhB5thgz1cjabr
eoOAdPicwUjndabSor9ylCTDpYpTc8SwuM9KoZyk39DNMUcW3DtwWVZ8YBcm5j0X
91+jp56NrKca0z1vcyRvy4Q=
-----END PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
MIIDazCCAlOgAwIBAgIUfo40Rk2dYhY8SO+yXL5vrvli+20wDQYJKoZIhvcNAQEL
BQAwRTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM
GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDAeFw0yMDA1MjExMzA4MzRaFw0yMTA1
MjExMzA4MzRaMEUxCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEw
HwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQDeItDNfMko2uLegVQUzPTLXx2hod9LKIN+S01TpwJv
Li5185JZ8YhquwZnI19VvLQLyOIUMGeVxVvgGPgkRA58liO4oyCjZGYJygwAN0gk
MvJXeMXuNeezlfm3xiVvCk0+l6Vb8FqyJod/P9sr6CyfQPolXSngZ8C7M/Fz3YWQ
4O26L9in79eynCeJodXmzHsdrLHwKKOhv3F9Chp5Rx82KRvoFrdhmUEGM+l4Aq1J
1iUVEsAQHHiMWhJhfsiC1GjGErOjSfWKw9e0ckbgqY/2ypXHdjsg0mI3eLO3E29S
nG8OCWDLRcJVU1oHeX8hqk04c0urXhVQThN3X6ecXD1dAgMBAAGjUzBRMB0GA1Ud
DgQWBBRXK63AaxqKc92abM3L9tM/sF1fmTAfBgNVHSMEGDAWgBRXK63AaxqKc92a
bM3L9tM/sF1fmTAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQC8
1/JSufP8yWnKWDXYrfWCM1ji+COiW3qrjeYxOyl6uvkJDDNFUt8MQUO2c4HFr4BE
I7BGYbCfGT3dc1K3/JKtlGeoKqbKMgBWe+Lu12kkB5nrQdyqTVSgQnL1HHN7u7ED
apSV9TzYcz6wbX4Yv27UMGpwbUypIG2EUVbBCkElZYoMn4TNlKF7uTH5dOmR+LNr
zGvTvYkjMFLRtJ13SkRyfiMJkfJcM89czOVu4X/dljiHhGePfdbCUuGs1Gw759a8
3l7b506sujWQEmuSe6UdOUws+gR82H7kb8n7qxcOa5HXiIE2MRdfHXx8AS0LGPsa
n0PAUDF7eqI/kYskiWUX
-----END CERTIFICATE-----
                 """)

    mysql = _in('Enable MYSQL API ? [Y/N]','Y',use_default)
    if mysql in ['Y','y']:
        config['api']['mysql'] = {
            "certificate_path": crt_path
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
        config['integrations']['default_clickhouse']['enabled'] = True
        config['integrations']['default_clickhouse']['host'] = _in('Clickhouse host: ','localhost',use_default)
        config['integrations']['default_clickhouse']['port'] = _in('Clickhouse port: ',8123,use_default)
        config['integrations']['default_clickhouse']['user'] = _in('Clickhouse user: ','default',use_default)
        config['integrations']['default_clickhouse']['password'] = _in('Clickhouse password: ','',use_default)
        config['integrations']['default_mariadb']['type'] = 'clickhouse'

    mariadb = _in('Connect to Mariadb ? [Y/N]','Y',use_default)
    if mariadb in ['Y','y']:
        config['integrations']['default_mariadb']['enabled'] = True
        config['integrations']['default_mariadb']['host'] = _in('Mariadb host: ','localhost',use_default)
        config['integrations']['default_mariadb']['port'] = _in('Mariadb port: ',3306,use_default)
        config['integrations']['default_mariadb']['user'] = _in('Mariadb user: ','root',use_default)
        config['integrations']['default_mariadb']['password'] = _in('Mariadb password: ','',use_default)
        config['integrations']['default_mariadb']['type'] = 'mariadb'

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
