#!/bin/bash
# -------------------------------------CopyRight-------------------------------------
# Filename:da_pat_dwm_drop.sh
# Revision:
# Date:2021-09-11
# Author:EX-YOUXUAN001
# Description:da透传dwm表---删表.

export LANG="en_US.UTF-8"

function copy_da_pat_dwm() {
export export HADOOP_USER_NAME=ph_rc_default

dwm_name=$1
exportdir=/user/hive/warehouse/ph_dwm.db/${dwm_name}
dadir=/user/hive/warehouse/ph_rc_da.db/da_${dwm_name}

echo "++++++++++ph_rc_da.da_${dwm_name}透传删表开始++++++++++"
hql="
set mapreduce.job.queuename=root.ph_rc_queue;
drop table if exists ph_rc_da.da_${dwm_name};
create table if not exists ph_rc_da.da_${dwm_name} as select * from ph_dwm.${dwm_name};
"
echo ${hql}
echo "++++++++++源目录${exportdir}目标目录${dadir}++++++++++"

hive -e "${hql}"

if [ $? != 0 ];then
    echo "++++++++++ph_rc_da.da_${dwm_name}透传删表失败++++++++++"
    exit 1
fi
echo "++++++++++ph_rc_da.da_${dwm_name}透传删表成功++++++++++"

}

copy_da_pat_dwm $1