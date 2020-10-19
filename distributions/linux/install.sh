#!/bin/bash
set -e
set -o pipefail

cmdcol="$(tput sgr0)$(tput bold)"
normalcol="$(tput sgr0)"
trap 'echo -n "$normalcol"' DEBUG

echo -e """
\e[38;5;35m
          ____________________
        /░                    ---------________
      /░                                       --_
    /░                                            --_
   /░                                                --_
  /░                                                    --_
 /░                                         __--__         --_
|░                                      __--       ---___      _
|░                                     -       /|      ---___-
|░         _________________          |      /░  |
|░        /              |░           |     /░    |
|░       /                |░         |     /░      |
 |░     /       / \        |░        |     \░      |
 |░    /       /░   \       |░      |       \░     |
 |░    /        \░   \       |░     |        \░    |
 |░___/          \░___\       |░___|          \░___|

           █▀▄▀█ ░▀░ █▀▀▄ █▀▀▄ █▀▀ █▀▀▄ █▀▀▄
           █░▀░█ ▀█▀ █░░█ █░░█ ▀▀█ █░░█ █▀▀▄
           ▀░░░▀ ▀▀▀ ▀░░▀ ▀▀▀░ ▀▀▀ ▀▀▀░ ▀▀▀░
$cmdcol


"""

printf "Installing MindsDb requires Python 3.6+ and pip. We will now attempt to detect their paths."
# Attempt to detect python
python_path="$(which python3)"
pip_path="$(which pip3)"
if [ -z "${python_path}" ]
then
    python_path="$(which python)"
fi

if [ -z "${pip_path}" ]
then
    pip_path="$(which pip)"
fi

printf "Detected python: $python_path\ndetected pip: $pip_path"

# Check that it's indeed python 3.6 and that pip works
${python_path} -c "import sys; print('Sorry, MindsDB requires Python 3.6+') and exit(1) if sys.version_info < (3,6) else exit(0)"
${pip_path} --version > /dev/null 2>&1

export MDB_INSTALL_PYTHONPATH="$python_path"
export MDB_INSTALL_PIPPATH="$pip_path"

cmdcol="$(tput sgr0)$(tput bold)"
normalcol="$(tput sgr0)"
trap 'echo -n "$normalcol"' DEBUG

echo -e """
This might take a few minutes (dozens of minutes ?, no longer than half an hour, pinky promise).
Go grab a coffee or something and wait for the inevitable error log 99% of the way through

\e[38;5;35m

_,-||*||-~*)
(*~_=========\

|---,___.-.__,\

|        o     \ ___  _,,,,_     _.--.
\      -^-    /*_.-|~      *~-;*     \

 \_      _  ..                 *,     |
   |*-                           \.__/
  /                      ,_       \  *.-.
 /    .-~~~~--.            *|-,   ;_    /
|              \               \  | ****
 \__.--.*~-.   /_               |.
            ***  *~~~---..,     |
                         \ _.-.*-.
                            \       \

                             ..     /
                               *****
$cmdcol

"""

temp_file=$(mktemp)
trap "rm -f $temp_file" 0 2 3 15 # Making sure the file is deleted after script finishes

# Python code below
cat << EOF > $temp_file
#!$python_path

import os
import sys
import time

python_path = sys.argv[1]
pip_path = sys.argv[2]
home = os.path.expanduser("~")
mdb_home = os.path.join(home, 'mindsdb')
try:
    os.makedirs(mdb_home)
except FileExistsError:
    pass

print(f'\nInstalling some large dependencies via pip ({pip_path}), this might take a while\n')
time.sleep(1)

retcode = os.system(f'{pip_path} install mindsdb')
if retcode != 0:
    raise Exception("Command exited with error")

time.sleep(1)
print('Done installing dependencies')
print('\nLast step: Configure Mindsdb\n')

from mindsdb.utilities.wizards import daemon_creator

daemon_path = daemon_creator(python_path)
print(f"Created daemon service config {daemon_path}")

exec_path = str(os.path.join(mdb_home, 'run'))

text = '\n'.join([
  '#!/bin/bash',
  f'{pip_path} install mindsdb --upgrade',
  f'{python_path} -m mindsdb',
])

with open(exec_path, 'w') as fp:
    fp.write(text)

os.system(f'chmod +x {exec_path}')

print(f"Created executable at {exec_path}")

print('Installation complete!')

print(f'You can use Mindsdb by running {exec_path}. Or by importing it as a python package.')

EOF
#/Python code

chmod 755 $temp_file

INSTALLER_SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

"${MDB_INSTALL_PYTHONPATH}" "$temp_file" "$MDB_INSTALL_PYTHONPATH" "$MDB_INSTALL_PIPPATH"