#!/bin/bash


# MindsDB Daemon script

daemonName="MindsDB"

pidDir="$HOME/.mindsdb/pid"
mkdir $pidDir 2> /dev/null

pidFile="$pidDir/$daemonName.pid"
touch $pidFile 2> /dev/null


logDir="$HOME/.mindsdb/logs"
# To use a dated log file.
# logFile="$logDir/$daemonName-"`date +"%Y-%m-%d"`".log"
# To use a regular log file.
logFile="$logDir/$daemonName.log"

# Log maxsize in KB
logMaxSize=1024   # 1mb

pid_main=0

doCommands() {
  # This is how we start mindsdb.


  echo """
             █▀▄▀█ ░▀░ █▀▀▄ █▀▀▄ █▀▀ █▀▀▄ █▀▀▄
             █░▀░█ ▀█▀ █░░█ █░░█ ▀▀█ █░░█ █▀▀▄
             ▀░░░▀ ▀▀▀ ▀░░▀ ▀▀▀░ ▀▀▀ ▀▀▀░ ▀▀▀░

            Starting server on http://127.0.0.1:47334 ...

  """
  
  cd $HOME/.mindsdb/
  source $HOME/.mindsdb/mindsdb/bin/activate
  python -m mindsdb & > "$logDir/$daemonName.log"

  #my-app &
  pid_main=$!

  echo "pkill -P $pid_main" > "$pidDir/$daemonName.pid"
  echo "$pid_main" > "$pidDir/$daemonName.pidn"
  tail -f "$logDir/$daemonName.log"

}

upgrade() {
   # simply do mindsdb upgrade
   /usr/local/opt/python@3.7/bin/pip3 install mindsdb --upgrade;

}




################################################################################
# Below is the skeleton functionality of the daemon. DO not touch
################################################################################


startDaemon() {
  # Start the daemon.
  if [ -f $pidDir/$daemonName.pidn ]; then
    status_pid="$(cat -s $pidDir/$daemonName.pidn)"
    status="$(ps $status_pid)"
    if [[ "$status" == *"$status_pid"* ]]; then
      echo "MindsDB is currently running."
    else
      loop
    fi
  else
    loop
  fi



}

stopDaemon() {
  # Stop the daemon.
  echo "Echo stopping MindsDB server.."
  curl -s http://localhost:47334/api/util/shutdown > /dev/null
  sh "$pidDir/$daemonName.pid"
  sleep 10
  echo "Done."

}

statusDaemon() {
  # Query and return whether the daemon is running.
  status_pid="$(cat -s $pidDir/$daemonName.pidn)"
  status="$(ps $status_pid)"

  if [[ "$status" == *"$status_pid"* ]]; then
    echo "MindsDB is currently running."
  else
    echo "MindsDB is NOT up currently."
  fi
  curl http://localhost:47334/api/util/ping
}

restartDaemon() {
  stopDaemon
  sleep 5
  startDaemon
}

loop() {
  # This is the loop.
  now=`date +%s`

  if [ -z $last ]; then
    last=`date +%s`
  fi

  # Do everything you need the daemon to do.
  doCommands

}

log() {
  # Generic log function.
  echo "$1" >> "$logFile"
}


################################################################################
# Parse the command.
################################################################################

if [ -f "$pidFile" ]; then
  oldPid=`cat "$pidFile"`
fi

case "$1" in
  start)
    startDaemon
    ;;
  stop)
    stopDaemon
    ;;
  status)
    statusDaemon
    ;;
  restart)
    restartDaemon
    ;;
  upgrade)

    upgrade

    ;;
  *)
  echo "MindsDB Server usage  { start | stop | restart | status | upgrade }"
  exit 1
esac

exit 0
