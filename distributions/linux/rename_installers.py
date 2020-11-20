import os
import sys

if sys.argv[1] == 'beta':
    filename = 'MindsDB_Linux_Beta.sh'
elif sys.argv[1] == 'release':
    filename = 'MindsDB_Linux_Latest.sh'

os.system('mkdir distributions/linux/dist')
os.system('cp distributions/linux/install.sh distributions/linux/dist/')
os.system('mv distributions/linux/dist/install.sh distributions/linux/dist/{}'.format(filename))
