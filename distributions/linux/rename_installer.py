import os

filename = 'MindsDB-Server-Latest-Setup.sh'
os.system('cd distributions/linux && mkdir dist')
os.system('cd distributions/linux && mv install.sh dist')
os.system('cd distributions/linux/dist && mv install.sh {}'.format(filename))
