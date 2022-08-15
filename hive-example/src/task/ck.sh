#!/bin/bash
d=`date +%Y%m%d%H%M`

#需要指定clickhouse 的服务器ip
clickhouse-client -h 127.0.0.1 --database='ph_rc_dws' --query="
select * from ph_rc_dws.rc_dws_ck_overdue_state_info_01 t final
where t.product_no in ('0','103','21','51') FORMAT CSV" >start_info_${d}.csv