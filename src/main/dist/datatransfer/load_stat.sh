#!/bin/bash

ROOT_DIR="/datatransfer/stat/"
cd $ROOT_DIR

for file in `ls $ROOT_DIR `
do
       echo $file
       loadDataFile=$ROOT_DIR$file
       loadCommand="load data local infile '$loadDataFile' into table A1.transfer_log fields terminated BY ',' lines terminated BY '\n' (process_date, channel_id, file_name, file_size);"
       echo $loadDataFile
       mysql --local-infile=1 -uroot -h localhost -P 3306 A1 -e "$loadCommand"
       rm -f $file
done
