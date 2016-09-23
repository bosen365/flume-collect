#!/bin/bash
USAGE="-e Usage: agent-daemon.sh start [agent-name] [conf-file] | stop"

FLUME_HOME=$(cd `dirname $0`; pwd)
BIN=$FLUME_HOME/bin/flume-ng
FlUME_PID_DIR=.
FlUME_PID=${FlUME_PID_DIR}/pid
LOG=restart.log

source $FLUME_HOME/functions.sh

function initialize_default_directories() {
   if [[ ! -d "${FlUME_PID_DIR}" ]]; then
    echo "Pid dir doesn't exist, create ${FlUME_PID_DIR}"
    $(mkdir -p "${FlUME_PID_DIR}")
  fi
}

check() {
  echo "now start check service"
  while :
  do
      if [ -f $FLUME_PID ] ; then
        mypid=`cat $FlUME_PID`
        cmdex="ps uh -p $mypid"
        psrtn=`$cmdex`
        echo "pid info: " $psrtn
        if [ -z "$psrtn" ]; then
            echo "`date '+%Y/%m/%d %H:%M:%S'` FATALERROR RESTART SERVICE" >> $LOG
            stop
            start
        fi
    fi
    echo "sleep >>>>>>>>>> 10h"
    sleep 6h
  done
}

start(){
    local pid
    if [[ -f "${FLUME_PID}" ]]; then
    pid=$(cat ${FlUME_PID})
    if kill -0 ${pid} >/dev/null 2>&1; then
      echo "agent is already running"
      return 0;
    fi
  fi

  initialize_default_directories

  echo "----------------config-------------------"
  echo "agent-name: $1"
  echo "config-file: $2"
  echo "----------------config-------------------"
  echo ""
  nohup nice -n 0 $BIN agent -n $1 -c conf -f $2 >> agent.log 2>&1 < /dev/null &
  pid=$!
  if [[ -z "${pid}" ]]; then
    action_msg "agent start" "${SET_ERROR}"
    return 1;
  else
    action_msg "agent start" "${SET_OK}"
    echo ${pid} > ${FlUME_PID}
    #sleep 20s
    #check
  fi
}


function stop() {
  local pid

  if [[ ! -f "${FlUME_PID}" ]]; then
    echo "agent is not running"
  else
    pid=$(cat ${FlUME_PID})
    if [[ -z "${pid}" ]]; then
      echo "agent is not running"
    else
      wait_for_agent_to_die $pid 40
      $(rm -f ${FlUME_PID})
      action_msg "agent stop" "${SET_OK}"
    fi
  fi
}

function wait_for_agent_to_die() {
  local pid
  local count
  pid=$1
  timeout=$2
  count=0
  timeoutTime=$(date "+%s")
  let "timeoutTime+=$timeout"
  currentTime=$(date "+%s")
  forceKill=1

  while [[ $currentTime -lt $timeoutTime ]]; do
    $(kill ${pid} > /dev/null 2> /dev/null)
    if kill -0 ${pid} > /dev/null 2>&1; then
      sleep 3
    else
      forceKill=0
      break
    fi
    currentTime=$(date "+%s")
  done

  if [[ forceKill -ne 0 ]]; then
    $(kill -9 ${pid} > /dev/null 2> /dev/null)
  fi
}

case "${1}" in
  start)
    start $2 $3
    ;;
  stop)
    stop
    ;;
  restart)
    stop
    start
    ;;
  *)
    echo ${USAGE}
esac
