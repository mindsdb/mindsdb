import sys
import os
from pathlib import Path
import time
from os.path import expanduser


install_as  = sys.argv[1]
python_path = sys.argv[2]
pip_path    = sys.argv[3]
default_install = sys.argv[4]
make_exec = sys.argv[5]
home = expanduser("~")
mdb_home = os.path.join(home, 'mindsdb1')

default_install = False if default_install.lower() == 'n' else True
make_exec = False if make_exec.lower() == 'n' else True


if install_as == 'user':
    config_dir = os.path.join(mdb_home,'data', 'config')
    storage_dir = os.path.join(mdb_home,'data', 'storage')
else:
    config_dir = os.path.join('/etc/mindsdb/')
    storage_dir = os.path.join('/var/lib/mindsdb/')

os.makedirs(config_dir,exist_ok=True)
os.makedirs(storage_dir,exist_ok=True)

print(f'Configuration files will be stored in {config_dir}')
print(f'Datasources and predictors will be stored in {storage_dir}')

datasource_dir = os.path.join(storage_dir,'datasources')
predictor_dir = os.path.join(storage_dir,'predictors')
# @TODO Deal with permissions stuff

os.makedirs(datasource_dir,exist_ok=True)
os.makedirs(predictor_dir,exist_ok=True)

print(f'\nInstalling some large dependencies via pip ({pip_path}), this might take a while\n')
time.sleep(3)

# Consider adding:  --force-reinstall
# How it installs itself (maybe instead of github just use local download if user has cloned everything ?)
if install_as == 'user':
    os.system(f'{pip_path} install --user git+https://github.com/mindsdb/mindsdb.git@split --upgrade')
else:
    os.system(f'sudo {pip_path} install git+https://github.com/mindsdb/mindsdb.git@split --upgrade')

branch = 'master' # In the future this will be `stable`
dataskillet_source = None
lightwood_source = f'git+https://github.com/mindsdb/lightwood.git@{branch}'
mindsdb_source = f'git+https://github.com/mindsdb/mindsdb.git@{branch}'

for source in [dataskillet_source,lightwood_source,mindsdb_source]:
    if isinstance(source,str):
        if install_as == 'user':
            os.system(f'{pip_path} install --user {source} --upgrade')
        else:
            os.system(f'sudo {pip_path} install {source} --upgrade')
time.sleep(1)
print('Done installing dependencies')
print('\nLast step: Configure Mindsdb\n')

from mindsdb.utilities.wizards import cli_config,daemon_creator,make_executable
config_path = cli_config(python_path,pip_path,predictor_dir,datasource_dir,config_dir,use_default=default_install)

if install_as == 'user':
    pass
else:
    daemon_creator(python_path,config_path)

if make_exec:
    if install_as == 'user':
        path = str(os.path.join(mdb_home,'run'))
    else:
        path = '/usr/bin/mindsdb'

    make_executable(python_path,config_path,path)

print('Installation complete !')

if make_exec:
    print(f'You can use Mindsdb by running {path}. Or by importing it as the `mindsdb` library from within python. <Some message about mindsdb Scout @Richie opinions on how we install this bit ?>')
else:
    print(f'You can use Mindsdb by running {python_path} -m mindsdb --config={config_path}. Or by importing it as the `mindsdb` library from within python. <Some message about mindsdb Scout @Richie opinions on how we install this bit ?>')
