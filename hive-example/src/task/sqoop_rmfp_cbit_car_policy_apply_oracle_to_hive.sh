#!/bin/sh
############################################################################
#*项目名称:	  rmfp_cbit_car_policy_apply表(oracle的ods层)推送到rmfp_cbit_car_policy_apply表(hive)
#*作业名:		  rmfp_cbit_car_policy_apply
#*创建时间:	  2021-10-14
#*负责人:		  EX-YOUXUAN001
############################################################################


sqoop import	 -D mapreduce.job.queue.name=root.ph_rc_queue \
				       -D org.apache.sqoop.splitter.allow_text_splitter=true \
				       --connect 'jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(HOST=phyxdcl.db.paic.com.cn)(PROTOCOL=TCP)(PORT=1534))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=srvp_phyxdc_rmfp_1)))' \
				       --username rbdppsqpw \
				       --password 'lqaz!WSX' \
				       --table RMFPDATA.RMFP_CBIT_CAR_POLICY_APPLY \
				       --hcatalog-database ph_rc_ods \  #要导入到hive库的hive的库名
				       --hcatalog-table RMFP_CBIT_CAR_POLICY_APPLY \  #要导入到hive库的hive的表名
				       --m 2



task_run $? "RMFPDATA.RMFP_CBIT_CAR_POLICY_APPLY(oracle的ods) to RMFP_CBIT_CAR_POLICY_APPLY(hive)"
echo "#############script sqoop_rmfp_cbit_car_policy_apply_ods_to_hive.sh successfully finished #############"