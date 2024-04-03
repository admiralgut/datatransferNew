#!/bin/bash

PROC_COUNT=`ps -ef |grep DataTransfer |grep -v grep |wc -l`

#echo $PROC_COUNT
if [[ $PROC_COUNT = 0 ]]; then
   source /etc/profile;
   /usr/bin/sh /datatransfer/start_datatransfer.sh
   echo -e "$(date '+%Y-%m-%d_%H:%M:%S') - DataTransfer start by guard!"
else
   echo -e "$(date '+%Y-%m-%d_%H:%M:%S') - DataTransfer OK!"
fi

