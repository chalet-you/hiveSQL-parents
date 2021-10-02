-- 创建ods层的hive表
-- 写入时压缩生效
set hive.exec.orc.compression.strategy=compression;
use itcast_ods;
drop table if exists itcast_ods.web_chat_ems;


create external table if not exists itcast_ods.web_chat_ems
(
    id                           int        comment '主键',
    create_date_time             string     comment '数据创建时间',
    session_id                   string     comment 'sessionId',
    sid                          string     comment '访客id',
    create_time                  string     comment '会话创建时间',
    seo_source                   string     comment '搜索来源',
    seo_keywords                 string     comment '关键字',
    ip                           string     comment 'IP地址',
    area                         string     comment '地域',
    country                      string     comment '所在国家',
    province                     string     comment '省',
    city                         string     comment '城市',
    origin_channel               string     comment '投放渠道',
    user_match                   string     comment '所属坐席',
    manual_time                  string     comment '人工开始时间',
    begin_time                   string     comment '坐席领取时间 ',
    end_time                     string     comment '会话结束时间',
    last_customer_msg_time_stamp string     comment '客户最后一条消息的时间',
    last_agent_msg_time_stamp    string     comment '坐席最后一下回复的时间',
    reply_msg_count              int        comment '客服回复消息数',
    msg_count                    int        comment '客户发送消息数',
    browser_name                 string     comment '浏览器名称',
    os_info                      string     comment '系统名称'
)comment '访问会话信息表'
partitioned by (starts_time string)
row format delimited fields terminated by '\t'
stored as orc
location '/user/hive/warehouse/itcast_ods.db/web_chat_ems_ods'
tblproperties ('orc.compress' = 'ZLIB');