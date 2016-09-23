#!/bin/bash

FlUME_PID=pid
LOG=restart.log
START_SH=start.sh
bin_path=$(cd `dirname $0`; pwd)

start(){
   cd $bin_path
   sh agent-daemon.sh start a1 conf/mg-spool.conf
}

check() {
    cd $bin_path
    if [ -f $FLUME_PID ] ; then
        mypid=`cat $FlUME_PID`
        cmdex="ps uh -p $mypid"
        psrtn=`$cmdex`
        if [ -z "$psrtn" ]; then
            echo "`date '+%Y/%m/%d %H:%M:%S'` FATALERROR RESTART SERVICE" >> $LOG
            start
        fi
    else
        break
    fi
    exit
}

check

