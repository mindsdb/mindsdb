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

# Attempt to detect python
python_path="$(which python3)"
pip_path="$(which pip3)"
printf "Detected python3: $python_path\ndetected pip3: $pip_path\nDo you want to use these [Y] or manually provide paths [N]? [Y/N]"
read use_detected_python
if [ "$use_detected_python" = "N" ] || [ "$use_detected_python" = "n" ]; then
    echo "Please enter the path to your python (3.6+) interpreter:"
    read python_path

    echo "Please enter the path to your associate pip installation:"
    read pip_path
fi
export MDB_INSTALL_PYTHONPATH="$python_path"
export MDB_INSTALL_PIPPATH="$pip_path"

# Check that it's indeed python 3.6 and that pip works
${python_path} -c "import sys; print('Sorry, MindsDB requires Python 3.6+') and exit(1) if sys.version_info < (3,6) else exit(0)"
${pip_path} --version > /dev/null 2>&1

echo "Do you want us to install using the default parameters [Y] or should we go ahead and give you control to tweak stuff during installation ? [Y/N]"
read default_install
export MDB_DEFAULT_INSTALL="$default_install"

export MDB_MAKE_EXEC="Y"
if [ "$EUID" -ne 0 ]; then
  install_as="user"
else
  install_as="global"
fi

if [ "$default_install" = "N" ] || [ "$default_install" = "n" ]; then
  if [ "$EUID" -ne 0 ]; then
      install_as="user"
      echo "You are currently installing Mindsdb for your user only, rather than globally. Is this intended ? [Y/N]"
      read approve
      if [ "$approve" = "N" ] || [ "$approve" = "n" ]; then
          echo "Please run the installer using sudo in front of the command"
          exit
      fi
    else
      install_as="global"
      echo "You are currently installing Mindsdb globally (as root), is this intended ? [Y/N]"
      read approve
      if [ "$approve" = "N" ] || [ "$approve" = "n" ]; then
          echo "Please run the installer as your desired user instead (without using sudo in front of it)"
          exit
      fi
  fi

  echo "Should we make an executable for mindsdb (in /usr/bin/ if installing as root or in your home directory if install as user)? [Y/N]"
  read make_exec
  export MDB_MAKE_EXEC="$make_exec"
fi


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

INSTALLER_SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

"${MDB_INSTALL_PYTHONPATH}" "$INSTALLER_SCRIPT_DIR"/install.py "$install_as" "$MDB_INSTALL_PYTHONPATH" "$MDB_INSTALL_PIPPATH" "$MDB_DEFAULT_INSTALL" "$MDB_MAKE_EXEC"
