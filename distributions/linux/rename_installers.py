import os
import sys

if sys.argv[1] == 'beta':
    filename = 'MindsDB_Linux_Beta.sh'
elif sys.argv[1] == 'release':
    filename = 'MindsDB_Linux_Latest.sh'

os.system('cd distributions/linux && mkdir dist')
os.system('cd distributions/linux && cp install.sh dist/')
os.system('cd distributions/linux/dist && mv install.sh {}'.format(filename))
