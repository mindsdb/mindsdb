import os
import sys

about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)
version = about['__version__']


if sys.argv[1] == 'beta':
    filename = 'beta_version.txt'
elif sys.argv[1] == 'release':
    filename = 'stable_version.txt'

os.system('mkdir -p distributions/version/dist')

with open(f'distributions/version/dist/{filename}', 'w') as fp:
    fp.write(version)
