#!/bin/bash
# -------------------------------------CopyRight-------------------------------------
# Filename:da_pat_dwm.sh
# Revision:
# Date:2021-09-11
# Author:EX-YOUXUAN001
# Description:da透传dwm表

export LANG="en_US.UTF-8"

function copy_da_pat_dwm() {
export export HADOOP_USER_NAME=ph_rc_default

dwm_name=$1
exportdir=/user/hive/warehouse/ph_dwm.db/${dwm_name}
dadir=/user/hive/warehouse/ph_rc_da.db/da_${dwm_name}

echo "++++++++++ph_dwm.${dwm_name}透传开始++++++++++"
hql="
set mapreduce.job.queuename=root.ph_rc_queue;
create table if not exists ph_rc_da.da_${dwm_name} like ph_dwm.${dwm_name};
truncate table ph_rc_da.da_${dwm_name};
dfs -cp ${exportdir}/* ${dadir}/;
msck repair table ph_rc_da.da_${dwm_name};
"
# dfs：用于直接在Hive执行HDFS的操作
# msck repair table ph_rc_da.da_${dwm_name};  用于hive的分区修复
echo ${hql}
echo "++++++++++源目录${exportdir}目标目录${dadir}++++++++++"

hive -e "${hql}"

if [ $? != 0 ];then
    echo "++++++++++ph_dwm.${dwm_name}透传失败++++++++++"
    exit 1
fi
echo "++++++++++ph_dwm.${dwm_name}透传至ph_rc_da.da_${dwm_name}成功++++++++++"

}

copy_da_pat_dwm $1