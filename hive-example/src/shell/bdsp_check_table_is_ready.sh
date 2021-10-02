#!/bin/bash
# -------------------------------------CopyRight-------------------------------------
# Filename:bdsp_check_table_is_ready.sh
# Revision:
# Date:2021-09-11
# Author:EX-YOUXUAN001
# Description:检测源数据任务是否完成

export LANG="en_US.UTF-8"

yesterday=`date -d "1 day ago" "+%Y-%m-%d"`
today=`date "+%Y-%m-%d"`

function wait_table_sqoop() {
  hadoop_user=$1
  db_name=$2
  table_name=$3
# 看替换的定义，${varname:-word}，如果varname存在且非null，则返回其值；否则，返回word。用途：如果变量未定义，则返回默认值。
# 变量替换     ${varname:-word} ,如果变量varname存在且非null，则返回其值，否则，返回word
# 此处的意思是：如果参数4存在那么就返回参数4，否则返回参数 yesterday
  sign_time=${4:-${yesterday}}

  # 第四个参数为 today，则取当天的日期标记success，其余情况取昨天
  if [ $sign_time = "today" ];then
    sign_time=$today
  else
    sign_time=$yesterday
  fi

  export HADOOP_USER_NAME=$hadoop_user
  lb_success_path=/user/$hadoop_user/legend/flag/${db_name}/${table_name}/${sign_time}/_success
  echo $lb_success_path
  #判断这个_success文件是否存在，如果以下这个命令正常执行，用$?可以获取到0，否则非0
  hdfs dfs -test -e $lb_success_path
  while [ $? -ne 0 ]
  do
    echo "${db_name}.${table_name} in ${sign_name} is not ready yet .${lb_success_path} does not exist !!"
    current_date=`date -d "0 day ago" "+%s"`
    assign_date=`date -d "0 day ago" "+%Y-%m-%d 09:30:00"`
    assign_timestamp=`date -d "$assign_date" +%s`
    sleep 60
    hdfs dfs -test -e $lb_success_path
  done
  echo "${db_name}.${table_name} is ready!!"
}

wait_table_sqoop $1 $2 $3 $4