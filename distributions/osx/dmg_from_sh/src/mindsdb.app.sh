#!/bin/bash
# This script is the main script in the MindsDB Server.app
# it is in charge of either installing mindsdb or launching mindsdb

MINDSDB_VERSION="$(cat mindsdb.installer.sh | grep  MINDSDB_VERSION= | tr '=' '\n' | grep -v MINDSDB_VERSION)"

installed_version="$(cat $HOME/.mindsdb/installed)"

# if the installer version is different from the version that was used to install mindsdb, reinstalling
# else launch mindsdb server

if [[ "$installed_version" == "$MINDSDB_VERSION" ]]; then
  echo "MindsDB is currently installed!."
  osascript -e 'tell application "Terminal" to do script "/$HOME/.mindsdb/mindsdb.sh start"'

else
  # make sure mindsdb is not currently running
  if [ ! -f "$HOME/.mindsdb/mindsdb.sh"  ]
  then
    echo "MindsDB is currently installed, stopping it and reinstalling."
    $HOME/.mindsdb/mindsdb.sh stop
  else
    echo "MindsDB is currently NOT installed!."
  fi

  echo "Install MindsDB ..."

  # Extract installer into destination path
  rm -rf $HOME/.mindsdb
  mkdir $HOME/.mindsdb

  mkdir $HOME/.mindsdb
  mkdir $HOME/.mindsdb/logs
  cp mindsdb*.sh $HOME/.mindsdb/
  chmod +x $HOME/.mindsdb/mindsdb*.sh

  #launch installation in Terminal
  osascript -e 'tell application "Terminal" to do script "/$HOME/.mindsdb/mindsdb.installer.sh"'

fi
