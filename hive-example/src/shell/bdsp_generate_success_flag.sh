#!/bin/bash
# -------------------------------------CopyRight-------------------------------------
# Filename:bdsp_generate_success_flag.sh
# Revision:
# Date:2021-09-11
# Author:EX-YOUXUAN001
# Description:生成任务完成标记

export LANG="en_US.UTF-8"

yesterday=`date -d "1 day ago" "+%Y-%m-%d"`
today=`date "+%Y-%m-%d"`

function generate_success_flag() {
  hadoop_user=$1
  db_name=$2
  table_name=$3
  # 看替换的定义，${varname:-word}，如果varname存在且非null，则返回其值；否则，返回word。用途：如果变量未定义，则返回默认值。
  flag_date=${4:-${yesterday}}


  if [ $flag_date = "today" ];then
    flag_date=$today
  elif [ $flag_date = "yesterday" ];then
    flag_date=$yesterday
  fi

  export HADOOP_USER_NAME=$hadoop_user
  lb_success_path=/user/$hadoop_user/legend/flag/${db_name}/${table_name}/${flag_date}
  lb_success_file=$lb_success_path/_SUCCESS

  hdfs dfs -mkdir -p $lb_success_path
  if [ $? -ne 0 ];then
    echo "********** create success flag path failed for table $lb_success_path **********"
    exit 1
  fi

  hdfs dfs -touchz $lb_success_file
  if [ $? -ne 0 ];then
   echo "********** create success flag file failed for table $lb_success_file **********"
   exit 1
  fi
  echo "================================task $lb_success_file has finished================================"
}

generate_success_flag $1 $2 $3 $4