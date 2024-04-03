#!/bin/bash



PID=`ps -ef | grep com.run.UploadStatMain |grep -v grep |awk '{ print $2}'`


if [[ "" !=  $PID ]]; then
   echo "killing " $PID
   kill $PID
fi
echo "stoped"
