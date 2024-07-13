#!/bin/bash
d=`date +%Y%m%d%H%M`

#需要指定clickhouse 的服务器ip，把ck中的表数据查询出来，然后导入到csv文件中，如果csv文件要
#带表头的话，把 CSV 改成 CSVWithNames
#FORMAT CSVWithNames
#final 让ck中的数据最终保持一致性。表示只查最新的数据。final是在新版本中是多线程查询的，final是跟在表名后面的哦
#final不支持MergeTree引擎的，但是ReplacingMergeTree，SummingMergeTree，CollapsingMergeTree引擎都支持
clickhouse-client -h 127.0.0.1 --database='ph_rc_dws' --query="select * from ph_rc_dws.rc_dws_ck_overdue_state_info_01 t final where t.product_no in ('0','103','21','51') FORMAT CSV" > start_info_${d}.csv