set hive.exec.mode.local.auto=false;
use itcast_dws;
insert into itcast_dws.visit_dws partition (yearinfo, monthinfo, dayinfo)
select count(distinct sid)                            as sid_total,
       count(distinct session_id)                     as sessionid_total,
       count(distinct ip)                             as ip_total,
       area,
       '-1'                                           as seo_source,
       '-1'                                           as origin_channel,
       '-1'                                           as hourinfo,
       quarterinfo,
       concat(yearinfo, '-', monthinfo, '-', dayinfo) as time_str,
       '-1'                                           as from_url,
       '1'                                            as grouptype,
       '2'                                            as time_type,
       yearinfo,
       monthinfo,
       dayinfo
from itcast_dwd.visit_consult_dwd
where concat(yearinfo, monthinfo, dayinfo) = '20190701'
  and instr(area, "中国") > 0
group by area, yearinfo, quarterinfo, monthinfo, dayinfo;