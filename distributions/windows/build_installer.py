import os

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)

filename = 'MindsDB-Server-{}-Setup.exe'.format(about['__version__'])
os.system('cd distributions/windows && pyinstaller install.py -F --onefile -n {}'.format(filename))