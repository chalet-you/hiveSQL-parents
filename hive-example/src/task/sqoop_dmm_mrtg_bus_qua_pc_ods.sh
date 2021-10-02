#!/bin/sh
############################################################################
#*项目名称:	  dmm_mrtg_bus_qua表(hive)推送到dmm_mrtg_bus_qua表(ods)
#*作业名:		  dmm_mrtg_bus_qua
#*创建时间:	  2021-10-14
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
# 如果执行此脚本不传递$1参数时间的话，就默认执行昨天的数据（此$1是全局的变量）,否则处理指定的日期的数据
# -n：not null，不为空,所以(! -n "$1")是代表第一个参数如果为空
if [ ! -n "$1" ] ;then
  processdate=`date -d "1 month ago" +"%Y%m"`
else
  processdate=$1
fi
##获取当前登录的用户名称
curuser=`id -nu`
##获取当前机子的ip地址
curip=`ifconfig | grep inet | grep netmask | grep broadcast`
sqoop_version=`sqoop version`

echo "======================================================"
echo "processdate:${processdate}"
echo "curuser:${curuser}"
echo "curip:${curip}"
echo "sqoop_version:${sqoop_version}"
echo "======================================================"

#oracle的用户名，队列名、库名、表名
username='phbdspsqpw'
queueName="ph_rc_queue"
tableName=cfodstdata.dmm_mrtg_bus_qua
database=cfodstdata
dirTableName=dmm_mrtg_bus_qua
srcTableName=dmm_mrtg_bus_qua

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




##查询ods是否有当天数据
echo "----------------开始查询ods对应${processdate}分区数据量----------------"
sqoop eval	--connect "${connectionStr}" \
			      --username ${username} \
			      --password-alias sqoop.oracle.cfods.${username} \
			      --query "select count(1) from ${tableName} where report_date='${processdate}'" > /var/applications/ph_rc_bdsp/tasks/da/da_rad_ddr_daily_log/${srcTableName}_log.txt
																																		
# tail -n 1 是获取文件的尾部数据，1是文件最后一行的数据
# grep '^|'表示首字母为“|”的行，linux里的文件
# awk '{print $2}'是 一行一行的读取指定的文件， 以空格或者制表符作为分隔符，打印第二个字段
count=`cat /var/applications/ph_rc_bdsp/tasks/da/da_rad_ddr_daily_log/${srcTableName}_log.txt | grep '^|' | tail -n 1 | awk '{print $2}'`
echo "================================"$count"================================"

if [ $count -ne 0 ];then
  echo "ods${processdate}分区存在，需要删除${processdate}分区数据"
# sqoop eval的命令可以让sqoop使用sql语句对数据库进行操作，如查询表结构，删除数据等
	sqoop eval	--connect "${connectionStr}" \
				      --username ${username} \
				      --password-alias sqoop.oracle.cfods.${username} \
				      --query "delete from ${tableName} where report_date='${processdate}';"
	task_run $? "delete from ${tableName} where report_date='${processdate}';"
						
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
				      --hcatalog-table ${srcTableName} \
				      --hcatalog-partition-keys report_date \
				      --hcatalog-partition-values ${processdate} \
				      --columns credit_apply_no,apply_no,cust_no,credit_line,fund_name,loan_amt,pay_type,loan_date,repay_cnt,repay_amt,create_by,date_created,updataed_by,date_updated,report_date \
				      --m 1 \
				      --batch \
				      --input-fields-terminated-by '\t \
				      --input-lines-terminated-by '\n' \
				      --input-null-string '\\\\N' \
				      --input-null-non-string '\\\\N'

				
				
task_run $? "dmm_plm_ocd_upl_migration_rate_base(hive) to dmm_mrtg_bus_qua(ods)"
echo "#############script sqoop_dmm_mrtg_bus_qua_pc_ods.sh successfully finished #############"