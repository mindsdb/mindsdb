import os
import sys

if sys.argv[1] == 'beta':
    filename = 'MindsDB_OSX_Beta.sh'
elif sys.argv[1] == 'release':
    filename = 'MindsDB_OSX_Latest.sh'

os.system('mkdir distributions/osx/dist')
os.system('cp distributions/linux/install.sh distributions/osx/dist/')
os.system('mv distributions/osx/dist/install.sh distributions/osx/dist/{}'.format(filename))
