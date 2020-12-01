import os
import sys

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
else:
    exit('Expected sys.argv[1] to be "beta" or "release"')

with open('distributions/windows/latest.py', 'w+') as f:
    f.write(install_py.replace('$name', LATEST_NAME).replace('$version', ''))

with open('distributions/windows/fixed.py', 'w+') as f:
    f.write(install_py.replace('$name', FIXED_NAME).replace('$version', about['__version__']))

os.system('cd distributions/windows && pyinstaller latest.py -F --onefile -n {}-Setup.exe'.format(LATEST_NAME))
os.system('cd distributions/windows && pyinstaller fixed.py -F --onefile -n {}-Setup.exe'.format(FIXED_NAME))
