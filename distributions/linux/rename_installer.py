import os

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)

filename = 'MindsDB-Server-{}-Setup.sh'.format(about['__version__'])
os.system('cd distributions/linux && mkdir dist')
os.system('cd distributions/linux && mv install.sh dist')
os.system('cd distributions/linux/dist && mv install.sh {}'.format(filename))
