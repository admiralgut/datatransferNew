CREATE DEFINER=`root`@`%` PROCEDURE `proc_aggr_transfer_log_history_v230601`()
BEGIN
	#Routine body goes here...
	#create table test(c1 int, c2 text);
	
	#DECLARE end_date BIGINT;
	#DECLARE stime TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
	
	SET @stime = CURRENT_TIMESTAMP;
	SELECT IFNULL(max(last_date), 0) into  @last_date FROM aggregate_log WHERE `status` = 0 ;
	SET @end_date = UNIX_TIMESTAMP(CURDATE());
	
	#select @last_date, @end_date, @stime;
	
	IF @end_date > IFNULL(@last_date,0) THEN
	 insert into converge_detail (
		statistic_date
		,data_type
		,data_source
		,dataset
		,center
		,city_code
		,recent_converge_time
		,recent_send_time
		,converge_package_num
		,converge_size
		,send_package_num
		,send_size
		,filter_package_num
		,filter_size)
	SELECT DATE_FORMAT(FROM_UNIXTIME(send_time),'%Y%m%d') as statistic_date
		, a.type_code
		, a.src_code
		, a.dataset
		, '1' as center
		, a.city_code
		, FROM_UNIXTIME(max(receive_time)) as recent_converge_time
		, FROM_UNIXTIME(max(send_time)) as recent_send_time
		, count(1) as converge_package_num
		, sum(file_size) as converge_size 
		, sum(case when status = 1 then 1 else 0 end) as send_package_num
		, sum(case when status = 1 then file_size else 0 end) as send_size
		, sum(case when status = 2 then 1 else 0 end) as filter_package_num
		, sum(case when status = 2 then file_size else 0 end) as filter_size
	FROM transfer_log a
	WHERE send_time >= @last_date and send_time < @end_date
	group by type_code , src_code, city_code, dataset, DATE_FORMAT(FROM_UNIXTIME(send_time),'%Y%m%d');
	 
	 insert into aggregate_log(last_date,start_time,end_time,status) values(@end_date,@stime,CURRENT_TIMESTAMP,0);

	END IF;


END
