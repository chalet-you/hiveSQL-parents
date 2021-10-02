#!/bin/bash
# -----------------------------CopyRight-----------------------------
# Filename:bdsp_exec_hql.sh
# Revision:
# Date:2021-09-09
# Author:EX-YOUXUAN001
# Description:执行hql.

export LANG="en_US.UTF-8"

# 昨天的年-月-日
yday=`date -d "last day" +%Y-%m-%d`
# 昨日所属的年-月
yday_month=`date -d "$yday" +%Y-%m`

# 年-月-日，上个月最后一天
LAST_DATE=`date "+%Y-%m-%d" -d "-$(date +%d) days"`
# 年-月-日，上个月
firstday=`date +%Y-%m-01`
LAST_MONTH=`date -d "$firstday 1 day ago" +%Y-%m`

#LAST_MONTH=`date -d "1 month ago" +%Y-%m`
# 年-月，上上个月
TWO_MONTH_AGO=`date -d "2 month ago" +%Y-%m`
# 现对于数据月的上一年
LAST_YEAR_DATE=`date -d "13 month ago" +%Y`
# 年-月-日，去年最后一天
LAST_YEAR=${LAST_YEAR_DATE}"-01"
#上个月的1号
FIRST_LAST_MONTH=${LAST_MONTH}"-01"
# 年-月-日，上上个月最后一天
LAST_TWO_MONTH_AGO=`date -d "$FIRST_LAST_MONTH last day" +%Y-%m-%d`

#集团公司 202010
DATE_MONTH_CORP=`date -d "1 month ago" +%Y%m`
function exec_hql() {
	hadoop_user=$1
	hql=$2

	export HADOOP_USER_NAME=$hadoop_user

	hive -f $hql -hiveconf lastmonth=${LAST_MONTH} -hiveconf TWO_MONTH_AGO=${TWO_MONTH_AGO} -hiveconf LAST_YEAR=${LAST_YEAR} -hiveconf FIRST_LAST_MONTH=${FIRST_LAST_MONTH} -hiveconf lastdate=${LAST_DATE} -hiveconf LAST_TWO_MONTH_AGO=${LAST_TWO_MONTH_AGO} -hiveconf yday_mth=${yday_month} -hiveconf yday=${yday}
	if [ $? -ne 0 ];then
		echo "************************ exec ${hql} failed ************************"
		exit 1
	fi


}

exec_hql $1 $2