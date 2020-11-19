import os
import sys

if sys.argv[1] == 'beta':
    filename = 'MindsDB_OSX_Beta.sh'
elif sys.argv[1] == 'release':
    filename = 'MindsDB_OSX_Latest.sh'

os.system('cd distributions/linux && mkdir dist')
os.system('cd distributions/linux && cp ../linux/install.sh dist/')
os.system('cd distributions/linux/dist && mv install.sh {}'.format(filename))
