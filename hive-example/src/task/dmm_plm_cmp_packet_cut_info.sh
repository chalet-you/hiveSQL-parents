#!/bin/sh
############################################################################
#*项目名称:	  CMP切包分案信息
#*作业名:		  dmm_plm_cmp_packet_cut_info
#*创建时间:	  2021-10-14
#*负责人:		  EX-YOUXUAN001
############################################################################

function task_run(){
	status=$1
	task_name=$2
	if [ ${status} -eq 0 ];then
		echo "任务执行成功，日期:${current_date},任务名称:${task_name}"
	else
		echo "任务执行失败,日期:${current_date},任务名称:${task_name}"
		exit 1
	fi
}

export HADOOP_USER_NAME=ph_rc_default

current_date=`date +"%Y-%m-%d"`
last_month=`date -d '1 month ago' +"%Y-%m"`   #上个月
late_last_month=$(date -d "`date +%Y-%m-01` 1 days ago" +%Y-%m-%d)  #上个月末
late_last_last_month=$(date -d "`date -d '1 month ago' +%Y-%m-01` 1 days ago" +%Y-%m-%d)  #上上月末

#hive的配置
hive_set="
set hive.execution.engine=mr;
set mapreduce.map.memory.mb=4096;
set mapreduce.job.queueName=root.ph_rc_queue;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrct;
set mapreduce.output.fileoutputformat.compress=false;
set mapreduce.task.io.sort.mb=1024;
set mapreduce.input.fileinputformat.split.maxsize=134220228;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.map.aggr.hash.percentmemory=0.25;
set mapreduce.reduce.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx3072m;
set mapreduce.reduce.java.opts=-Xmx3072m;
set hive.exec.reducers.bytes.per.reducer=536880912;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=536870912;
set hive.merge.smallfiles.avgsize=1048576;
set hive.auto.convert.join=false;	--默认已经开启了Map Join,这边需要关掉
set hive.groupby.skewindata=true;
set mapred.job.name=ph_rc_dmm.dmm_plm_cmp_packet_cut_info;
"

hql1="
${hive_set}
use ph_rc_dmm;
insert overwrite table ph_rc_dmm.dmm_plm_cmp_packet_cut_info partition (stat_date='${current_date}')
select pkg.apply_no as apply_no,
    cut_pkg_time,
    business_type,
    sales_org_id,
    sales_org_name,
    plb_pkg_org_id_lmt,
    plb_pkg_org_name_lmt,
    src_postloan_perf_pkg_org_id,
    src_postloan_perf_pkg_org_name,
    (case when substr(cut_pkg_time,1,7) = '${last_month}' then llm_loan_act_bal
          when substr(cut_pkg_time,1,7) = substr('${current_month}',1,7) then lm_loan_bal
          end) as loan_bal, --剩余本金
    tag_type,
    risk_level,
    'ph_bdsp' as created_by,
    current_timestamp() as create_time,
    'ph_bdsp' as updated_by,
    current_timestamp() as updated_time
from (select apply_no,
             cut_pkg_time,
             business_type,
             sales_org_id,
             sales_org_name,
             src_postloan_perf_pkg_org_id,    --切包归属id
             src_postloan_perf_pkg_org_name,   --切包归属名称
             tag_type,
             risk_level
       from ph_dwm.dwm_mod_cmp_pkg_cut_info
       where stat_date = '${current_date}' and cut_pkg_time is not null)pkg
left join (select apply_no,
                  src_postloan_perf_pkg_org_id as plb_pkg_org_id_lmt,   --上月末切包归属id
                  src_postloan_perf_pkg_org_name as plb_pkg_org_name_lmt,   --上月末切包归属名称
            from(select apply_no,
                        src_postloan_perf_pkg_org_id,
                        src_postloan_perf_pkg_org_name,
                        row_number()over(partition by apply_no order by cut_pkg_time desc)rn
                  from ph_dwm.dwm_mod_cmp_pkg_cut_info
                  where stat_date = '${current_date}' and cut_pkg_time is not null and date_format(cut_pkg_time,'yyyy-MM') = '${last_month}') t
            where t.rn = 1        --因为9月份才有切包时间，所以这里上月末的时间只取10月份对应的上月
            ) pkg_lmt
on pkg.apply_no = pkg_lmt.apply_no
left join (select apply_no,
                  max(case when stat_date = '${late_last_month}' then loan_bal else 0 end) as lm_loan_bal,      --上月末剩余本金
                  max(case when stat_date = '${late_last_last_month}' then loan_bal else 0 end) as llm_loan_bal     --上上月末剩余本金
           from ph_dwm.dwm_mod_postloan_status_daily_slice_09
           when stat_date in ('${late_last_month}','${late_last_last_month}')
           group by apply_no) sli     --取上月末和上上月末数据
on pkg.apply_no=sli.apply_no
;
"

# 查询当日是否有数据
hal="
set mapreduce.job.queueName=root.ph_rc_queue;
select count(1) from ph_dwm.dwm_mod_cmp_pkg_cut_info where stat_date ='${current_date}' and substr(cut_pkg_time,1,0) = '${current_date}';
"

hql2="
${hive_set}
use ph_rc_dmm;
insert overwrite table ph_rc_dmm.dmm_plm_cmp_packet_cut_info partition (stat_date='${current_date}')
select pkg.apply_no as apply_no,
       cut_pkg_time,
       business_type,
       sales_org_id,
       sales_org_name,
       plb_pkg_org_id_lmt,
       plb_pkg_org_name_lmt,
       src_postloan_perf_pkg_org_id,
       src_postloan_perf_pkg_org_name,
       loan_bal,
       tag_type,
       risk_level,
       'ph_bdsp' as created_by,
       current_timestamp() as create_time,
       'ph_bdsp' as updated_by,
       current_timestamp() as updated_time
from (select apply_no,
             cut_pkg_time,
             business_type,
             sales_org_id,
             sales_org_name,
             src_postloan_perf_pkg_org_id,      --切包归属id
             src_postloan_perf_pkg_org_name,    --切包归属名称
             tag_type,
             risk_level,
       from ph_dwm.dwm_mod_cmp_pkg_cut_info
       where stat_date = '${current_date}' and substr(cut_pkg_time,1,10) = '${current_date}') pkg
left join (select apply_no,
                  src_postloan_perf_pkg_org_id as plb_pkg_org_id_lmt,
                  src_postloan_perf_pkg_org_name as plb_pkg_org_name_lmt
            from (select apply_no,
                         src_postloan_perf_pkg_org_id,
                         src_postloan_perf_pkg_org_name,
                         row_number() over(partition by apply_no order by cut_pkg_time desc)as rn
                   from ph_dwm.dwm_mod_cmp_pkg_cut_info
                   where stat_date = '${current_date}' and cut_pkg_time is not null and substr(cut_pkg_time,1,7) = '${last_month}') t
            where t.rn = 1      --取上月最晚时间的切包归属
            ) pkg_lmt
on pkg_lmt.apply_no = pkg_lmt.apply_no
left join (select apply_no,
                  loan_bal
           from ph_dwm.dwm_mod_postloan_status_daily_slice_09
           where stat_date = '${late_last_month}') sli
on pkg.apply_no=sli.apply_no
;
"

# 初始化和增量导入数据进行分开
if [ ${current_date} = '2021-10-13' ];then
    echo "当前日期为：${current_date},开始初始化导入数据。。。"
    hive -e "${hql1}"
    if [ $? -eq 0 ];then
        echo "初始化导入完成"
    fi
else
    echo "开始查询${current_date}的切包数据。。。"
    count=`hive -e "${hql}"|tail -1`
    if [ ${count} -ne 0 ];then
      echo "${current_date} 数据量为${count}，开始导数。。。"
      hive -e "${hql2}"
      if [ $? -eq 0 ];then
        echo "导数成功"
      fi
    else
      echo "${current_date} 数据量为${count}，不需要导数，程序已退出。。。"
      exit 0
    fi
fi

# 使用sqoop推送数据

##获取当前登录的用户名称
curuser=`id -nu`
##获取当前机子的ip地址
curip=`ifconfig | grep inet | grep netmask | grep broadcast`
sqoop_version=`sqoop version`

echo "================================================================================"
echo "processdate:${processdate}"
echo "curuser:${curuser}"
echo "curip:${curip}"
echo "sqoop_version:${sqoop_version}"
echo "================================================================================"

username='phbdspsqpw'
queueName="root.ph_rc_queue"
tableName=cfodstdata.dmm_plm_cmp_packet_cut_info
database=cfodstdata
dirTableName=dmm_plm_cmp_packet_cut_info

ip=cfods.db.paic.com.cn
port=1531
filename=tmp_${tableName}_${current_date}.log
# 这里是用来判断环境的  || 表示或者   && 表示并且
# telnet  是用来测试远程服务器的端口是否开启，是否可以访问这个端口
env=`timeout 0.1 telnet $ip $port > $filename || cat $filename | grep scape | wc -l && rm -rf $filename`

#dwm抽数到cfods
if [ $env = '1' ];then
	connectionStr='jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=cfods.db.paic.com.cn)(PORT=1531))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=cfods)))'
else
	connectionStr='jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.31.10.152)(PORT=1553))(connect_data=(sid=t3cfods)))'
fi
echo "开始delete${srcTableName}当日数据----------------"

sqoop eval	--connect "${connectionStr}" \
			      --username ${username} \
			      --password-alias sqoop.oracle.cfods.${username} \
			      --query "delete from ${tableName} where stat_date = '${current_date}';"

task_run $? "delete from ${tableName} where stat_date = '${current_date}';"


sqoop export	-D mapreduce.job.queuename=$queueName \
				      --table ${tableName} \
				      --connect "${connectionStr}" \
				      --username ${username} \
				      --password-alias sqoop.oracle.cfods.${username} \
				      --hcatalog-database ph_rc_dmm \
				      --hcatalog-table dmm_plm_cmp_packet_cut_info \
				      --hcatalog-partition-keys stat_date \
				      --hcatalog-partition-values ${current_date} \
				      --columns apply_no,cut_pkg_time,biz_type,sales_org_id,sales_org_name,plb_pkg_org_id_lmt,plb_pkg_org_name_lmt,plb_pkg_org_id,plb_pkg_org_name,loan_bal,tag_type,risk_level,create_by,date_created,updataed_by,date_updated,stat_date \
				      --m 1 \
				      --batch \
				      --input-fields-terminated-by '\001' \
				      --input-lines-terminated-by '\n' \
				      --input-null-string '\\\\N' \
				      --input-null-non-string '\\\\N'
				
				
				
task_run $? "dmm_plm_cmp_packet_cut_info(hive) to dmm_plm_cmp_packet_cut_info(ods)"
echo "#############script dmm_plm_cmp_packet_cut_info.sh successfully finished #############"