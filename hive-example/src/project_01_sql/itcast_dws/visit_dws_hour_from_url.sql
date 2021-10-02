use itcast_dws;
insert into itcast_dws.visit_dws partition (yearinfo, monthinfo, dayinfo)
select count(distinct sid)                                           as sid_total,
       count(distinct session_id)                                    as sessionid_total,
       count(distinct ip)                                            as ip_total,
       '-1'                                                          as area,
       '-1'                                                          as seo_source,
       '-1'                                                          as origin_channel,
       hourinfo,
       quarterinfo,
       concat(yearinfo, '-', monthinfo, '-', dayinfo, ' ', hourinfo) as time_str,
       from_url,
       '4'                                                           as grouptype,
       '1'                                                           as time_type,
       yearinfo,
       monthinfo,
       dayinfo
from itcast_dwd.visit_consult_dwd
where concat(yearinfo, monthinfo, dayinfo) = '20190701'
  and length(from_url) > 0
group by from_url, yearinfo, quarterinfo, monthinfo, dayinfo, hourinfo;