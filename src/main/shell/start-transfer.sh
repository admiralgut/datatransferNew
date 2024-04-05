#!/bin/bash
# */10 * * * * nohup sh /opt/yanhong/stat_loadmpp.sh >/opt/yanhong/logs/stat_loadmpp.out 2>&1 &
SHELL_HOME=$(cd $(dirname $0);pwd)
DATATRANSFER_HOME=${SHELL_HOME}/datatransfer

echo -e "\n============================================"
echo -e "$(date '+%Y-%m-%d_%H:%M:%S') - DataTransfer begin"


#java -version
source /etc/profile
nohup java  -cp ${DATATRANSFER_HOME}/conf:${DATATRANSFER_HOME}/DataTransfer.jar -Dlog4j.dir=/home/app/log -Dlog4j.file=data-transfer.log com.run.DataTransfer >/dev/null 2>&1 &
#       java -cp ${DATATRANSFER_HOME}/conf:${DATATRANSFER_HOME}/DataTransfer.jar:${DATATRANSFER_HOME}/lib/* com.run.DataTransfer


echo -e "$(date '+%Y-%m-%d_%H:%M:%S') - DataTransfer end"
