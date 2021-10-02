--本地模式
set hive.exec.mode.local.auto=true;
--动态分区配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.created.files=150000;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=compression;


use itcast_dwd;
insert into table itcast_dwd.visit_consult_dwd partition (yearinfo, monthinfo, dayinfo)
select wce.session_id,
       wce.sid,
       unix_timestamp(wce.create_time, 'yyyy-MM-dd HH:mm:ss.SSS') as create_time,
       wce.seo_source,
       wce.ip,
       wce.area,
       cast(if(wce.msg_count is null, 0, wce.msg_count) as int)   as msg_count,
       wce.origin_channel,
       wcte.referrer,
       wcte.from_url,
       wcte.landing_page_url,
       wcte.url_title,
       wcte.platform_description,
       wcte.other_params,
       wcte.history,
       substr(wce.create_time, 12, 2)                             as hourinfo,
       ceil(substr(wce.create_time, 6, 2) / 3.0)                  as quarterinfo,
       substr(wce.create_time, 1, 4)                              as yearinfo,
       substr(wce.create_time, 6, 2)                              as monthinfo,
       substr(wce.create_time, 9, 2)                              as dayinfo
from itcast_ods.web_chat_ems wce
         inner join itcast_ods.web_chat_text_ems wcte
                    on wce.id = wcte.id;