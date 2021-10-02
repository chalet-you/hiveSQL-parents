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


use itcast_dws;
insert into table itcast_dws.visit_dws partition (yearinfo, monthinfo, dayinfo)
select count(distinct sid)                                           as sid_total,
       count(distinct session_id)                                    as sessionid_total,
       count(distinct ip)                                            as ip_total,
       '-1'                                                          as area,
       '-1'                                                          as seo_source,
       '-1'                                                          as origin_channel,
       hourinfo,
       quarterinfo,
       concat(yearinfo, '-', monthinfo, '-', dayinfo, ' ', hourinfo) as time_str,
       '-1'                                                          as from_url,
       '5'                                                           as grouptype,
       '1'                                                           as time_type,
       yearinfo,
       monthinfo,
       dayinfo
from itcast_dwd.visit_consult_dwd
where concat(yearinfo, monthinfo, dayinfo) = '20190701'
group by yearinfo, quarterinfo, monthinfo, dayinfo, hourinfo;