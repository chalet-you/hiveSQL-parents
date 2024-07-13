#!/bin/bash
############################################################################
#*项目名称:	  oracle 导数到 hive中
#*作业名:		  导数
#*创建时间:	  2021-08-22
#*负责人:		  EX-YOUXUAN001
############################################################################

export HADOOP_USER_NAME=ph_rc_default
tableName=$1
hive -e "truncate table ph_rc_ods.${tableName};"

sqoop import -D mapred.job.queue.name=root.ph_rc_queue \ # 设置队列
             --connect 'jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=30.4.4.104)(PORT=1536))(LOAD_BALANCE=yes)(FAILOVER=on)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=t5phyxdc)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=180)(DELAY=5))))' \
             --username pub_test \   # 指定要登入oracle的账号
             --password '5tgb^YHN' \ # 指定要登入oracle的密码
             --table RMFPDATA.${tableName} \ # 指定oracle的表名
             --hcatalog-database ph_rc_ods \ # 要导入到hive库的hive的库名
             --hcatalog-table ${tableName} \ # 要导入到hive库的hive的表名
             --where "date_updated > to_date('2022-10-01','yyyy-mm-dd')" \  # 要限定oracle中的date_updated的数据
             --m 1