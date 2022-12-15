#!/bin/bash
#############################
# 事务表查询数据
#############################

export HADOOP_USER_NAME=ph_rc_default

database=$1
tablename=$2
query=$3

hive_set="
set tez.queue.name=root.ph_rc_queue;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set tez.job.name=${tablename}_query;
use ${database};
"

hql="
${query};
"

hql="${hive_set}${hql}"
echo ${hql}

hive -e "${hql}"