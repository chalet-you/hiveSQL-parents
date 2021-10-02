-- 创建ods层的hive表
-- 写入时压缩生效
set hive.exec.orc.compression.strategy=compression;
use itcast_dwd;
drop table if exists itcast_ods.web_chat_text_ems;

create external table if not exists itcast_ods.web_chat_text_ems
(
    id                      int         comment '主键来自MySQL',
    referrer                string      comment '上级来源页面',
    from_url                string      comment '会话来源页面',
    landing_page_url        string      comment '访客着陆页面',
    url_title               string      comment '咨询页面title',
    platform_description    string      comment '客户平台信息',
    other_params            string      comment '扩展字段中数据',
    history                 string      comment '历史访问记录'
) comment 'EMS-PV测试表'
partitioned by (start_time STRING)
row format delimited fields terminated by '\t'
stored as orc
location '/user/hive/warehouse/itcast_ods.db/web_chat_text_ems_ods'
tblproperties ('orc.compress' = 'ZLIB');