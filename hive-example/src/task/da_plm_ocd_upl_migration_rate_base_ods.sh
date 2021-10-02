#!/bin/sh
############################################################################
#*项目名称:	  指挥舱-中间表
#*作业名:		  dmm_plm_ocd_upl_migration_rate_base
#*创建时间:	  2021-08-22
#*负责人:		  EX-YOUXUAN001
############################################################################

function task_run(){
	status=$1
	task_name=$2
	if [ ${status} -eq 0 ];then
		echo "任务执行成功，日期:${processdate},任务名称:${task_name}"
	else
		echo "任务执行失败,日期:${processdate},任务名称:${task_name}"
		exit 1
	fi
}

export HADOOP_USER_NAME=ph_rc_default

partition_date=`date -d "2 day ago 1 month ago" +"%Y-%m-%d %H:%M:%S"`

processdate=`date -d "1 day ago" +"%Y-%m-%d"`
pro_date=`date -d "1 day ago" +"%Y-%m-%d"`
curuser=`id -nu`
curip=`ifconfig | grep inet | grep netmask | grep broadcast`
sqoop_version=`sqoop version`

echo "================================================================================"
echo "processdate:${processdate}"
echo "curuser:${curuser}"
echo "curip:${curip}"
echo "sqoop_version:${sqoop_version}"
echo "================================================================================"


ip=cfods.db.paic.com.cn
port=1531
filename=tmp_${tableName}_${processdate}.log
env=`timeout 0.1 telnet $ip $port > $filename || cat $filename | grep scape | wc -l && rm -rf $filename`

#dwm抽数到cfods
if [ $env = '1' ];then
	connectionStr='jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=cfods.db.paic.com.cn)(PORT=1531))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=cfods)))'
else
	connectionStr='jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.31.10.152)(PORT=1553))(connect_data=(sid=t3cfods)))'
fi


username='phbdspsqpw'
queueName="root.ph_rc_queue"
tableName=cfodstdata.dmm_plm_upl_migr_rate_base
database=cfodstdata
dirTableName=dmm_plm_upl_migr_rate_base
exportdir=/user/hive/warehouse/ph_rc_dmm_db/dmm_plm_ocd_upl_migration_rate_base/report_date=${processdate}
dbName='ph_rc_dmm'
srcTableName=dmm_plm_ocd_upl_migration_rate_base


##drop一个月前的分区
sqoop eval	--connect "${connectionStr}" \
			      --username ${username} \
			      --password-alias sqoop.oracle.cfods.${username} \
			      --query "begin
			      				  APPMGR.PARTITION_OPERATE.drop_interval_part_by_time('${database}','${dirTableName}','${partition_date}');
			      				  end;"
					 
task_run $? "begin
					  APPMEGR.PARTITION_OPERATE.drop_interval_part_by_time('${database}','${dirTableName}','${partition_date}');
					  end;"
##查询ods是否有对应分区
echo "----------------------------------------------------------开始查询ods对应${processdate}分区数据量-----------------------------------------------------------------"
sqoop eval	--connect "${connectionStr}" \
			      --username ${username} \
			      --password-alias sqoop.oracle.cfods.${username} \
			      --query "select count(1) from ${tableName} where partition_date=date'${processdate}'" > /var/applications/ph_rc_bdsp/tasks/da/da_rad_ddr_daily_log/${srcTableName}_log.txt
																																		
# tail -n 1 是获取文件的尾部数据，1是文件最后一行的数据
# grep '^|'表示首字母为“|”的行，linux里的文件
# awk '{print $2}'是 一行一行的读取指定的文件， 以空格或者制表符作为分隔符，打印第二个字段
count=`cat /var/applications/ph_rc_bdsp/tasks/da/da_rad_ddr_daily_log/${srcTableName}_log.txt | grep '^|' | tail -n 1 | awk '{print $2}'`

echo "==================================================================================="$count"====================================================================================="

if [ $count -ne 0 ];then
  echo "ods${processdate}分区存在，需要删除${processdate}分区数据"
	sqoop eval	--connect "${connectionStr}" \
				      --username ${username} \
				      --password-alias sqoop.oracle.cfods.${username} \
				      --query "begin
								            APPMEGR.PARTITION_OPERATE.truncate_partition('${database}','${dirTableName}',PARTITION_OPERATE.GET_PARTITION_NAME('${database}','${dirTableName}','${processdate}'));
								            end;"
	task_run $? "begin
						APPMEGR.PARTITION_OPERATE.truncate_partition('${database}','${dirTableName}',PARTITION_OPERATE.GET_PARTITION_NAME('${database}','${dirTableName}','${processdate}'));
						end;"
						
else
	echo "ods${processdate}分区不存在，第一次导数，不需要删除分区"
fi
	
	
	
echo "---------------开始导入${processdate}数据---------------"



sqoop export	-D mapreduce.job.queuename=$queueName \
				      --table ${tableName} \
				      --connect "${connectionStr}" \
				      --username ${username} \
				      --password-alias sqoop.oracle.cfods.${username} \
				      --hcatalog-database ph_rc_dmm \
				      --hcatalog-table dmm_plm_ocd_upl_migration_rate_base \
				      --hcatalog-partition-keys report_date \
				      --hcatalog-partition-values ${processdate} \
				      --columns apply_no,channel,tier,branch,manage_city,m0_cmp_branch_lastmth,m0_cmp_branch,m1_cmp_branch,m2_cmp_branch,m1_cmp_branch_lastmth,m2_cmp_branch_lastmth,biz_line,product_org,product_no,product_type,loan_date,loan_act_bal,status_of_month,status_of_month_fcst,loan_act_bal_lastmth,overdue_days_lastmth,status_of_lastmonth,create_by,date_created,updataed_by,date_updated,report_date \
				      --m 1 \
				      --batch \
				      --input-fields-terminated-by '\001' \
				      --input-lines-terminated-by '\n' \
				      --input-null-string '\\\\N' \
				      --input-null-non-string '\\\\N'
				
				
				
task_run $? "dmm_plm_ocd_upl_migration_rate_base(hive) to dmm_plm_upl_migr_rate_base(ods)"
echo "#############script da_plm_ocd_upl_migration_rate_base_ods.sh successfully finished #############"