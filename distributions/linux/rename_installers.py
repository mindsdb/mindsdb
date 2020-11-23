import os
import sys

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)
version = about['__version__']


if sys.argv[1] == 'beta':
    filename = 'MindsDB_Linux_Beta.sh'
    versioned_filename = f'MindsDB_Linux_{version}_Beta.sh'
elif sys.argv[1] == 'release':
    filename = 'MindsDB_Linux_Latest.sh'
    versioned_filename = f'MindsDB_Linux_{version}.sh'

os.system('mkdir distributions/linux/dist')
os.system('cp distributions/linux/install.sh distributions/linux/dist/')
os.system(f'mv distributions/linux/dist/install.sh distributions/linux/dist/{filename}')

with open(f'distributions/linux/dist/{filename}', 'r') as fp:
    content = fp.read()
    content = content.replace('$version', version)

with open(f'distributions/linux/dist/{filename}', 'w') as fp:
    fp.write(content)

with open(f'distributions/osx/dist/{versioned_filename}', 'w') as fp:
    fp.write(content)
