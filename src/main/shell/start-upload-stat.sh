#!/bin/bash
# */10 * * * * nohup sh /opt/yanhong/stat_loadmpp.sh >/opt/yanhong/logs/stat_loadmpp.out 2>&1 &
SHELL_HOME=$(cd $(dirname $0);pwd)
DATATRANSFER_HOME=${SHELL_HOME}/datatransfer

echo -e "\n============================================"
echo -e "$(date '+%Y-%m-%d_%H:%M:%S') - upload stat start"


#java -version

nohup java -Dlog4j.dir=${SHELL_HOME}/log -Dlog4j.file=upload-stat.log -cp ${SHELL_HOME}/conf:${DATATRANSFER_HOME}/DataTransfer.jar:${DATATRANSFER_HOME}/lib/* com.run.UploadStatMain > ${SHELL_HOME}/log/upload-stat.out 2>&1 & 


echo -e "$(date '+%Y-%m-%d_%H:%M:%S') - upload stat stoped"
