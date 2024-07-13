--------------------------------------------
--- *项目名称    :回传信息--主表信息
--- *作业名      :qmx_sas_appl
--- *作业简介    :回传信息--主表信息
--- *创建时间    :2022-05-12
--- *负责人      :ex-youxuan001
--- *输入表      :ph_rc_qmx.qmx_sas_instinctresp
--- *输出表      :ph_rc_qmx.qmx_sas_appl
--- *更改时间    :
--- *更改人      :
--- *更改记录    :

set hive.execution.engine=tez;
set tez.queue.name=root.ph_rc_queue;
--动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=5000;
--任务并行
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
--小文件合并
set hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.size.per.task=256000000;
set hive.exec.mode.local.auto=false;
--自动mapjoin
set hive.auto.convert.join=false;
--数据倾斜优化
--set hive.groupby.skewindata=true;
--MapTask ReduceTask 内存调整
set mapreduce.map.java.opts=-Xmx4096m;
set mapreduce.map.memory.mb=32768;
set mapreduce.reduce.java.opts=-Xmx4096m;
set mapreduce.reduce.memory.mb=32768;
--map reduce 处理的数据量
set maprd.max.split.size=536870912;
set mapreduce.input.fileinputformat.split.minsize=536870912;
set mapred.min.split.size.per.node=536870912;
set mapred.min.split.size.per.rack=536870912;
set hive.exec.reducers.bytes.per.reducer=536870912;--每个Reduce处理的数据量大小，此处为 512M
--执行前小文件合并
set hive.input.format=or.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
--开启压缩
set hive.exec.compress.output=true;
set hive.exec.default.partition.name=default;

set tez.job.name=qmx_sas_appl;

use ph_rc_qmx;

insert overwrite table ph_rc_qmx.qmx_sas_appl partition (dt,pub_runprocess,pub_taskname,pub_datapackage,pub_strategypackage)
select * from ph_rc_qmx.qmx_app;