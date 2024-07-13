# 第一个脚本
sqoop import -D mapreduce.job.queuename=root.ph_rc_queue \
--connect jdbc:mysql://node3:3306/nev \
--username root \
--password 123456 \
--driver com.mysql.jdbc.Driver \
--query 'select id,referrer,from_url,landing_page_url,url_title,platform_description,other_params,history, "2019-07-01" as start_time from web_chat_text_ems_2019_07 where $CONDITIONS' \
--hcatalog-database itcast_ods \
--hcatalog-table web_chat_text_ems \
--hcatalog-storage-stanza 'stored as orc tblproperties ("orc.compress"="ZLIB")' \
-m 2 \
--split-by id



# 第二个脚本
hive -e 'truncate table ph_rc_ods.rmfp_rpa_trademark;'
sqoop import -D mapreduce.job.queuename=root.ph_rc_queue \
--connect 'jdbc:oracle:thin:@(description=(address=(host=phyxdcl.db.paic.com.cn)(protocol=tcp)(port=1534))(connect_data=(server=dedicated)(service_name=srvp_phyxdc_rmfp_1)))' \
--username rbdppsqpw \
--password 'lqaz!wsx' \
--table rmfp_rpa_trademark \
--hcatalog-database ph_rc_ods \
--hcatalog-table rmfp_rpa_trademark \
-m 1