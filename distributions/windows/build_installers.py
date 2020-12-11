import os
import sys
import requests
from PIL import Image

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)

with open('distributions/windows/install.py', 'r') as f:
    install_py = f.read()

if sys.argv[1] == 'beta':
    LATEST_NAME = 'MindsDB_Latest_Beta'
    FIXED_NAME = 'MindsDB_{}_Beta'.format(about['__version__'])
elif sys.argv[1] == 'release':
    LATEST_NAME = 'MindsDB_Latest'
    FIXED_NAME = 'MindsDB-{}'.format(about['__version__'])

with open('distributions/windows/latest.py', 'w+') as f:
    f.write(install_py.replace('$name', LATEST_NAME).replace('$version', ''))

with open('distributions/windows/fixed.py', 'w+') as f:
    f.write(install_py.replace('$name', FIXED_NAME).replace('$version', about['__version__']))

icon_path = 'distributions/windows/mdb-icon.png'

with open(icon_path, 'wb') as f:
    f.write(
        requests.get('https://mindsdb-installer.s3-us-west-2.amazonaws.com/mdb-icon.png').content
    )

img = Image.open(icon_path)
new_path = icon_path.rstrip('.png') + '.ico'
img.save(new_path, sizes=[(96, 96)])

icon_abspath = os.path.abspath(new_path)

os.system('cd distributions/windows && pyinstaller latest.py --icon="{}" -F --onefile --uac-admin -n {}-Setup.exe'.format(icon_abspath, LATEST_NAME))
os.system('cd distributions/windows && pyinstaller fixed.py --icon="{}" -F --onefile --uac-admin -n {}-Setup.exe'.format(icon_abspath, FIXED_NAME))
