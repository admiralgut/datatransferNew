#!/bin/bash



PID_TRANSFER=`ps -ef | grep com.run.DataTransfer |grep -v grep |awk '{ print $2}'`
PID_MONITOR=`ps -ef | grep com.run.MonitorMain |grep -v grep |awk '{ print $2}'`

if [[ ! $PID_TRANSFER ]]; then
   echo "restarting datatransfer"
   sh start_datatransfer.sh
fi

if [[ ! $PID_MONITOR ]]; then
   echo "restarting monitor"
   sh start_monitor.sh
fi
