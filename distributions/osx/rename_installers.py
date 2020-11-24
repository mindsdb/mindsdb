import os
import sys

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)
version = about['__version__']

if sys.argv[1] == 'beta':
    filename = 'MindsDB_OSX_Beta.sh'
    versioned_filename = f'MindsDB_OSX_{version}_Beta.sh'
elif sys.argv[1] == 'release':
    filename = 'MindsDB_OSX_Latest.sh'
    versioned_filename = f'MindsDB_OSX_{version}.sh'

os.system('mkdir distributions/osx/dist')
os.system('cp distributions/linux/install.sh distributions/osx/dist/')
os.system('mv distributions/osx/dist/install.sh distributions/osx/dist/{}'.format(filename))

with open(f'distributions/osx/dist/{filename}', 'r') as fp:
    content = fp.read()
    content = content.replace('$version', version)

with open(f'distributions/osx/dist/{filename}', 'w') as fp:
    fp.write(content)
    
with open(f'distributions/osx/dist/{versioned_filename}', 'w') as fp:
    fp.write(content)
