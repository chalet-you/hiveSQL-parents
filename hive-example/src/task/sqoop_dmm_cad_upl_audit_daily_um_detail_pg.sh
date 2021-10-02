#!/bin/sh
############################################################################
#*项目名称:	  审批看板
#*作业名:		  dmm_cad_upl_audit_daily_um_detail 推送到 pg 对应表 dmm_cad_upl_audit_daily_um_detail
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
# 如果执行此脚本不传递$1参数时间的话，就默认执行昨天的数据（此$1是全局的变量）,否则处理指定的日期的数据
# -n：not null，不为空,所以(! -n "$1")是代表第一个参数如果为空
if [ ! -n "$1" ] ;then
    processdate=`date -d "1 day ago" +"%Y-%m-%d"`
else
    processdate=$1
fi
#获取当前登录的用户名称
curuser=`id -nu`
curip=`ifconfig | grep inet | grep netmask | grep broadcast`
sqoop_version=`sqoop version`

echo "============================"
echo "processdate:${processdate}"
echo "curuser:${curuser}"
echo "curip:${curip}"
echo "sqoop_version:${sqoop_version}"
echo "============================"
# pg 的用户名还有队列名以及要导入到pg的哪一张表名中
username="bdspapi_bi"
queueName="ph_rc_queue"
tableName=dmm_cad_upl_audit_daily_um_detail

# 获取 pg 库的机子的ip地址
ip=PHBIANA-postgresql.db.paic.com.cn
port=3725
filename=tmp_${tableName}_${processdate}.log
env=`timeout 0.1 telnet $ip $port > $filename || cat $filename | grep scape | wc -l && rm -rf $filename`

#dwm 抽数到 pg
#判断执行的环境，1是生产环境，否则为测试环境
if [ $env = '1' ];then
	connectionStr='jdbc:postgresql://PHBIANA-postgresql.db.paic.com.cn:3725/phbiana'
else
	connectionStr='jdbc:postgresql://TOPHBIANA-postgresql.dbstg.paic.com.cn:3507/phbiana'
fi

# 如果昨天的 pg 表 有数据的话 truncate掉 pg 表的数据
sqoop eval	--connect "${connectionStr}" \
			      --username ${username} \
			      --password-alias sqoop.postgresql.phbiana.${username} \
			      --query "TRUNCATE TABLE ${tableName}"

task_run $? "TRUNCATE TABLE ${tableName}"
# 开始从 hive 表抽数到 pg 表中，注意hive的表的字段必须和pg表的字段名称必须一模一样，但字段的顺序可以不一样，是通过字段名来判断插入到 pg 表中的
sqoop export	-D mapreduce.job.queuename=$queueName \
				      --table ${tableName} \
				      --connect "${connectionStr}" \
				      --username ${username} \
				      --password-alias sqoop.postgresql.phbiana.${username} \
				      --hcatalog-database ph_rc_dmm \
				      --hcatalog-table dmm_cad_upl_audit_daily_um_detail \
				      --hcatalog-partition-keys report_date \
				      --hcatalog-partition-values ${processdate} \
				      --columns audit_um,audit_center,area,audit_group,audit_completed_cnt,audit_pass_cnt,audit_pass_rate,audit_back_cnt,audit_back_rate,audit_back_two_cnt,audit_back_two_rate,audit_income_cnt,audit_hang_up_cnt,audit_hang_up_rate,audit_hang_two_cnt,audit_hang_up_two_rate,operation_duration,end_to_end_duration,report_date \
				      --m 1 \
				      --batch \
				      --input-fields-terminated-by '\t' \
				      --input-lines-terminated-by '\n' \
				      --input-null-string '\\\\N' \
				      --input-null-non-string '\\\\N'




task_run $? "dmm_cad_upl_audit_daily_um_detail(hive) to dmm_cad_upl_audit_daily_um_detail(pg)"
echo "#################script sqoop_dmm_cad_upl_audit_daily_um_detail_pg.sh successfully finished #################"