use itcast_dws;
drop table if exists itcast_dws.visit_dws;
create table if not exists itcast_dws.visit_dws
(
    sid_total               int     comment '根据sid去重求count',
    sessionid_total         int     comment '根据sessionid去重求count',
    ip_total                int     comment '根据IP去重求count',
    area                    string  comment '区域信息',
    seo_source              string  comment '搜索来源',
    origin_channel          string  comment '来源渠道',
    hourinfo                string  comment '创建时间，统计至小时',
    quarterinfo             string  comment '季度',
    time_str                string  comment '时间明细',
    from_url                string  comment '会话来源页面',
    groupType               string  comment '产品属性类型：1.时间+地区；2.时间+搜索来源；3.时间+来源渠道；4.时间+会话来源页面；5.只是时间总访问量',
    time_type               string  comment '时间聚合类型：1、按小时聚合；2、按天聚合；3、按月聚合；4、按季度聚合；5、按年聚合；'
)
comment 'EMS访客日志dws表'
partitioned by (yearinfo string,monthinfo string,dayinfo string)
row format delimited fields terminated by '\t'
stored as orc
location '/user/hive/warehouse/itcast_dws.db/visit_dws'
tblproperties ('orc.compress' = 'SNAPPY');