#!/bin/bash



echo """
           █▀▄▀█ ░▀░ █▀▀▄ █▀▀▄ █▀▀ █▀▀▄ █▀▀▄
           █░▀░█ ▀█▀ █░░█ █░░█ ▀▀█ █░░█ █▀▀▄
           ▀░░░▀ ▀▀▀ ▀░░▀ ▀▀▀░ ▀▀▀ ▀▀▀░ ▀▀▀░


"""

echo "Installing ...."
echo $LOGO_NAME


brew_path="$(which brew)"
python_path="/usr/local/opt/python@3.7/bin/python3"

# if python 3.7 not installed, then try to install it

if [ ! -f "$python_path"  ]
then
    echo "You will need XCode installed"
    xcode-select --install
    if [ -z "${brew_path}" ]
    then
        echo "We will try to install python 3 via MacOS package manager brew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi
    brew_path="$(which brew)"
    if [ -z "${brew_path}" ]
    then
        echo "Was not able to install brew..."
        exit
    fi
    echo "Installing python3.7.."
    brew update
    brew install python@3.7
fi




# if python3.7 not installed anyways, exit

pip_path="/usr/local/opt/python@3.7/bin/pip3"

if [ -z "${python_path}" ]
then
    echo "No python installation, please try to install 'brew install python@3.7"
    exit

fi

if [ -z "${pip_path}" ]
then
    echo "No pip3.7"
    exit

fi

echo "Detected python: $python_path\ndetected pip: $pip_path"

# Check that it's indeed python 3.6 and that pip works
${python_path} -c "import sys; print('Sorry, MindsDB requires Python 3.6+') and exit(1) if sys.version_info < (3,6) else exit(0)"
${pip_path} --version > /dev/null 2>&1

export MDB_INSTALL_PYTHONPATH="$python_path"
export MDB_INSTALL_PIPPATH="$pip_path"

echo "Creating $HOME/.mindsdb"
mkdir $HOME/.mindsdb
mkdir $HOME/.mindsdb/logs
cd $HOME/.MindsDB

echo "Creating VENV..."

${python_path} -m venv mindsdb
source mindsdb/bin/activate


echo """
This might take a few minutes (dozens of minutes ?, no longer than half an hour, pinky promise).
Go grab a coffee or something and wait for the inevitable error log 99% of the way through



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

Installing MindsDB Python dependencies ...

"""


pip install mindsdb




echo "Installation complete!"

echo """


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

"""
echo > $HOME/.mindsdb/installed

sh $HOME/.mindsdb/mindsdb.sh start
