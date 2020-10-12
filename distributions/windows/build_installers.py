import os

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)

with open('distributions/windows/install.py', 'r') as f:
    install_py = f.read()

LATEST_NAME = 'MindsDB-Server-Latest'
FIXED_NAME = 'MindsDB-Server-{}'.format(about['__version__'])

with open('distributions/windows/latest.py', 'w+') as f:
    f.write(install_py.replace('$name', LATEST_NAME).replace('$version', ''))

with open('distributions/windows/fixed.py', 'w+') as f:
    f.write(install_py.replace('$name', FIXED_NAME).replace('$version', about['__version__']))

os.system('cd distributions/windows && pyinstaller latest.py -F --onefile -n {}-Setup.exe'.format(LATEST_NAME))
os.system('cd distributions/windows && pyinstaller fixed.py -F --onefile -n {}-Setup.exe'.format(FIXED_NAME))
