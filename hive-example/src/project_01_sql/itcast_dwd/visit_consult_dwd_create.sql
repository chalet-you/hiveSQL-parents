--写入时压缩生效
set hive.exec.orc.compression.strategy=compression;

use itcast_dwd;
drop table if exists itcast_dwd.visit_consult_dwd;

create table if not exists itcast_dwd.visit_consult_dwd
(
    session_id           string comment '会话id',
    sid                  string comment '访客id',
    create_time          bigint comment '会话创建时间',
    seo_source           string comment '搜索来源',
    ip                   string comment 'IP地址',
    area                 string comment '地域',
    msg_count            int    comment '客户发送消息数',
    origin_channel       string comment '来源渠道',
    referrer             string comment '上级来源页面',
    from_url             string comment '会话来源页面',
    landing_page_url     string comment '访客着陆页面',
    url_title            string comment '咨询页面title',
    platform_description string comment '客户平台信息',
    other_params         string comment '扩展字段中数据',
    history              string comment '历史访问记录',
    hourinfo             string comment '小时',
    quarterinfo          string comment '季度'
)comment '访问咨询DWD表'
partitioned by (yearinfo string, monthinfo string, dayinfo string)
row format delimited fields terminated by '\t'
stored as orc
location '/user/hive/warehouse/itcast_dwd.db/visit_consult_dwd'
tblproperties ('orc.compress' = 'SNAPPY');