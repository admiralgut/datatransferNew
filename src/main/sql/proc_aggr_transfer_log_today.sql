CREATE DEFINER=`root`@`%` PROCEDURE `proc_aggr_transfer_log_today_v230601`()
BEGIN
	#Routine body goes here...
	
	SET @today = UNIX_TIMESTAMP(CURDATE());
	
	delete from converge_detail where status=1;
	
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
		,filter_size
		,status)
	SELECT DATE_FORMAT(FROM_UNIXTIME(@today),'%Y%m%d') as statistic_date
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
		, 1 as status
	FROM transfer_log a
	WHERE send_time >= @today
	group by type_code , src_code, city_code, dataset;
	 

END

