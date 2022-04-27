#!/bin/bash

PHANTOMJS=/usr/local/bin/phantomjs
PIDFILE=/var/run/ganglia_snapshot.pid
LOGFILE=/var/log/ganglia_snapshot.log
SCRIPTFILE=$(dirname $0)/save_snapshot.js

function log() {
  echo $(date -Is -u) $@ >> $LOGFILE
}

if [ -f $PIDFILE ]; then
  PID=$(cat $PIDFILE)
  ps -p $PID &> /dev/null

  if [ $? -eq 0 ]; then
    log "Previous snapshot process $PID is still running, cleanup..."
    kill -9 $PID
  fi
fi

$PHANTOMJS $SCRIPTFILE &>> $LOGFILE &
PID=$!
log "Taking snapshot in process $PID ..."
echo $PID > $PIDFILE

