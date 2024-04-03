create database A7;
use A7;

drop table if exists `transfer_log`;
create table transfer_log (
    transfer_id int PRIMARY KEY AUTO_INCREMENT,
	process_date BIGINT not null COMMENT '处理时间',
	channel_id VARCHAR(30) not null comment '通道id',
	file_name text not null comment '文件名',
	file_size int not null comment '文件大小',
	status tinyint not null comment '文件状态'
);

/*insert into transfer_log (process_date,channel_id,file_name,file_size)
values(20230303104201,"channel_0001","baidu_1233434556566_123344_20230303.zip",34353234);*/

drop table if exists `transfer_log_day`;
create table transfer_log_day (
	process_date int not null COMMENT '处理日期',
	channel_id VARCHAR(30) not null comment '通道id',
	file_count int not null comment '文件数',
	file_size bigint not null comment '文件总大小',
	PRIMARY KEY(process_date,channel_id)
);


-- load data local infile "d:/test/transfer_12_1678107000.log"
-- into table transfer_log fields terminated by ',' lines terminated by '\n' (process_date, channel_id, file_name, file_size);


drop table if exists `monitor_log`;
create table monitor_log (
    monitor_id int PRIMARY KEY AUTO_INCREMENT,
	monitor_time timestamp not null COMMENT '时间',
	host_ip CHAR(15) not null comment 'ip',
	cpu_used float not null comment 'cpu 使用率',
	memery_used float not null comment '内存使用率',
	disk_used text not null comment '磁盘使用率'
);

