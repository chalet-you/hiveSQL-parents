#!bin/bash
#--------------CopyRight--------------
#Revision:

source ~/.bash_profile
export HADOOP_USER_NAME=hadoop;
export LANG="en_US.UTF-8";


hive -e "
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=4;
set hive.auto.convert.join=false;
set mapreduce.job.queuename=root.ph_rc_queue;
set mapreduce.job.name=dws_mod_cmp_unallot_coll_predict;
set v_stat_date=date_sub(current_date,1);
set hive.exec.temporary.table.storage=ssd;
use ph_dwm;

select tmp1.* from
(select tmp.*,row_number() over(partition by tmp.wx_id order by date_created desc)rn
from (select t.* from dwm_vm_ucp_emm_wx_msg_info t
inner join(
      select distinct a.cust_wx_id from dwm_vw_wrk_bse_wx_friend a
      where a.handle_sts = '3'
      and not exists(select 1
                from dwm_vw_wrk_bse_wx_friend a2,dwm_vw_wrk_bse_wx_rec tt
                where tt.collection_clerk_um =a2.collect_clerk_um
                and tt.collect_clerk_wx_mobile=a2.collect_clerk_wx_mobile
                and tt.cust_mobile_no=a2.mobile_no
                and a2.cust_wx_id=a.cust_wx_id)) t1 on t.wx_id=t1.cust_wx_id)tmp
)tmp1 where tmp1.rn=1 limit 3000
;" | sed 's/[\t]/,/g' > wx_talk.csv

echo 'Data Set is successesd to convert to gbk...'

# echo 'Data is ready to convert to gbk...'
# iconv -f utf-8 -t gbk -c unallot.csv -o unallot_out.csv

echo 'Data is ready to zip...'
zip -r wx_talk.zip wx_talk.csv

echo 'Data Set is successed to be processed...'
