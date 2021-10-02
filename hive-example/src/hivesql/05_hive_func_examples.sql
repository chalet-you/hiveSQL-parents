use itheima;
show tables;

--------------------------------多字节分隔符----------------------------------------------------

--针对双字节分隔符 采用默认的SerDe来处理
drop table itheima.singer;
create table singer
(
    id       string,
    name     string,
    country  string,
    province string,
    gender   string,
    works    string
)
    row format delimited fields terminated by '||';

--加载数据
load data local inpath '/root/hivedata/test01.txt' into table singer;

select *
from singer;


--情况二：数据的字段中包含了分隔符
drop table itheima.apachelog;
create table apachelog
(
    ip     string,
    stime  string,
    mothed string,
    url    string,
    policy string,
    stat   string,
    body   string
)
    row format delimited fields terminated by ' ';

load data local inpath '/root/hivedata/apache_web_access.log' into table apachelog;


select *
from apachelog;

------------------------
--清洗完数据之后  使用|分隔符
create table singer_wash
(
    id       string,
    name     string,
    country  string,
    province string,
    gender   string,
    works    string
)
    row format delimited fields terminated by '|';

load data local inpath '/root/hivedata/test01_wash.txt' into table singer_wash;

select *
from singer_wash;


------------------
--使用正则Regex来解析数据

--如果表已存在就删除表
drop table if exists singer;
--创建表
create table singer
(
    id       string,--歌手id
    name     string,--歌手名称
    country  string,--国家
    province string,--省份
    gender   string,--性别
    works    string
) --作品
--指定使用RegexSerde加载数据
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
        WITH SERDEPROPERTIES ("input.regex" = "([0-9]*)\\|\\|(.*)\\|\\|(.*)\\|\\|(.*)\\|\\|(.*)\\|\\|(.*)");

--加载数据
load data local inpath '/root/hivedata/test01.txt' into table singer;

select *
from itheima.singer;


--如果表存在，就删除表
drop table if exists apachelog;
--创建表
create table apachelog
(
    ip     string, --IP地址
    stime  string, --时间
    mothed string, --请求方式
    url    string, --请求地址
    policy string, --请求协议
    stat   string, --请求状态
    body   string  --字节大小
)
--指定使用RegexSerde加载数据
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
--指定正则表达式
        WITH SERDEPROPERTIES (
        "input.regex" = "([^ ]*) ([^}]*) ([^ ]*) ([^ ]*) ([^ ]*) ([0-9]*) ([^ ]*)"
        ) stored as textfile;


load data local inpath '/root/hivedata/apache_web_access.log' into table apachelog;

select *
from apachelog;

describe formatted apachelog;


----自定义InputFormat
add jar /root/HiveUserInputFormat.jar;


--如果表已存在就删除表
drop table if exists singer;
--创建表
create table singer
(
    id       string,--歌手id
    name     string,--歌手名称
    country  string,--国家
    province string,--省份
    gender   string,--性别
    works    string
)
--指定使用分隔符为|
    row format delimited fields terminated by '|'
--指定使用自定义的类实现解析
    stored as
        inputformat 'bigdata.itcast.cn.hive.mr.UserInputFormat'
        outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

--加载数据
load data local inpath '/root/hivedata/test01.txt' into table singer;

select *
from itheima.singer;


--------------------------------URL解析--------------------------------------------------------

SELECT parse_url('http://www.facebook.com/path/p1.php?id=10086', 'HOST');

SELECT parse_url_tuple('http://facebook.com/path/p1.php?id=10086&name=allen', 'PATH', 'QUERY') as (path, query);

SELECT parse_url('http://facebook.com/path/p1.php?id=10086&name=allen', 'QUERY', 'name');



drop table if exists tb_url;
--建表
create table tb_url
(
    id  int,
    url string
) row format delimited
    fields terminated by '\t';
--加载数据
load data local inpath '/root/hivedata/url.txt' into table tb_url;

select *
from tb_url;


select parse_url_tuple(url, "HOST", "PATH") as (host, path)
from tb_url;

select parse_url_tuple(url, "PROTOCOL", "HOST", "PATH") as (protocol, host, path)
from tb_url;

select parse_url_tuple(url, "HOST", "PATH", "QUERY") as (host, path, query)
from tb_url;


--parse_url_tuple
select id,
       parse_url_tuple(url, "HOST", "PATH", "QUERY") as (host, path, query)
from tb_url;


--单个lateral view使用
select a.id    as id,
       b.host  as host,
       b.path  as path,
       b.query as query
from tb_url a lateral view parse_url_tuple(url, "HOST", "PATH", "QUERY") b as host, path, query;

--多个lateral view
select a.id       as id,
       b.host     as host,
       b.path     as path,
       c.protocol as protocol,
       c.query    as query
from tb_url a
         lateral view parse_url_tuple(url, "HOST", "PATH") b as host, path
         lateral view parse_url_tuple(url, "PROTOCOL", "QUERY") c as protocol, query;

---Outer Lateral View
--如果UDTF不产生数据时，这时侧视图与原表关联的结果将为空
select id,
       url,
       col1
from tb_url
         lateral view explode(array()) et as col1;


--如果加上outer关键字以后，就会保留原表数据，类似于outer join
select id,
       url,
       col1
from tb_url
         lateral view outer explode(array()) et as col1;



select *
from tb_url;
--------------------------------hive行列转换-------------------------------------------------------

--1、多行转多列
--case when 语法1
select id,
       case
           when id < 2 then 'a'
           when id = 2 then 'b'
           else 'c'
           end as caseName
from tb_url;

--case when 语法2
select id,
       case id
           when 1 then 'a'
           when 2 then 'b'
           else 'c'
           end as caseName
from tb_url;


--建表
create table row2col1
(
    col1 string,
    col2 string,
    col3 int
) row format delimited fields terminated by '\t';
--加载数据到表中
load data local inpath '/root/hivedata/r2c1.txt' into table row2col1;

select *
from row2col1;

--sql最终实现
select col1                                         as col1,
       max(case col2 when 'c' then col3 else 0 end) as c,
       max(case col2 when 'd' then col3 else 0 end) as d,
       max(case col2 when 'e' then col3 else 0 end) as e
from row2col1
group by col1;


--2、多行转单列
select *
from row2col1;
select concat("it", "ca st", "And", "heima");
select concat("it", "cast", "And", null);

select concat_ws("-", "itcast", "And", "heima");
select concat_ws("-", "itcast", "And", null);

select collect_list(col1)
from row2col1;
select concat_ws(',', collect_list(col1))
from row2col1;

select collect_set(col1)
from row2col1;
select concat_ws(',', collect_set(col1))
from row2col1;


--建表
create table row2col2
(
    col1 string,
    col2 string,
    col3 int
) row format delimited fields terminated by '\t';

--加载数据到表中
load data local inpath '/root/hivedata/r2c2.txt' into table row2col2;

select *
from row2col2;

describe function extended concat_ws;

--最终SQL实现
select col1,
       col2,
       concat_ws(',', collect_list(cast(col3 as string))) as col3
from row2col2
group by col1, col2;


--3、多列转多行
select 'b', 'a', 'c'
union
select 'a', 'b', 'c'
union
select 'a', 'b', 'c';


--创建表
create table col2row1
(
    col1 string,
    col2 int,
    col3 int,
    col4 int
) row format delimited fields terminated by '\t';

--加载数据
load data local inpath '/root/hivedata/c2r1.txt' into table col2row1;

select *
from col2row1;

--最终实现
select col1, 'c' as col2, col2 as col3
from col2row1
UNION ALL
select col1, 'd' as col2, col3 as col3
from col2row1
UNION ALL
select col1, 'e' as col2, col4 as col3
from col2row1;


--4、单列转多行
select explode(split("a,b,c,d", ","));

--创建表
create table col2row2
(
    col1 string,
    col2 string,
    col3 string
) row format delimited fields terminated by '\t';

--加载数据
load data local inpath '/root/hivedata/c2r2.txt' into table col2row2;

select *
from col2row2;

select explode(split(col3, ','))
from col2row2;

--SQL最终实现
select col1,
       col2,
       lv.col3 as col3
from col2row2
         lateral view
             explode(split(col3, ',')) lv as col3;


--------------------------------hive json格式数据处理-------------------------------------------------------
--get_json_object
--创建表
create table tb_json_test1
(
    json string
);

--加载数据
load data local inpath '/root/hivedata/device.json' into table tb_json_test1;

select *
from tb_json_test1;


select
    --获取设备名称
    get_json_object(json, "$.device")     as device,
    --获取设备类型
    get_json_object(json, "$.deviceType") as deviceType,
    --获取设备信号强度
    get_json_object(json, "$.signal")     as signal,
    --获取时间
    get_json_object(json, "$.time")       as stime
from tb_json_test1;

--json_tuple
--单独使用
select
    --解析所有字段
    json_tuple(json, "device", "deviceType", "signal", "time") as (device, deviceType, signal, stime)
from tb_json_test1;

--搭配侧视图使用
select json,
       device,
       deviceType,
       signal,
       stime
from tb_json_test1
         lateral view json_tuple(json, "device", "deviceType", "signal", "time") b
         as device, deviceType, signal, stime;


--JsonSerDe
--创建表
drop table if exists tb_json_test2;
create table tb_json_test2
(
    device     string,
    deviceType string,
    signal     double,
    time       string
)
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    STORED AS TEXTFILE;

load data local inpath '/root/hivedata/device.json' into table tb_json_test2;

select *
from tb_json_test2;


--------------------------------hive 窗口函数应用实例-------------------------------------------------------
--1、连续登陆用户
--建表
create table tb_login
(
    userid    string,
    logintime string
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/login.log' into table tb_login;

select *
from tb_login;

--自连接过滤实现
--a.构建笛卡尔积
select a.userid    as a_userid,
       a.logintime as a_logintime,
       b.userid    as b_userid,
       b.logintime as b_logintime
from tb_login a,
     tb_login b;

--上述查询结果保存为临时表
create table tb_login_tmp as
select a.userid    as a_userid,
       a.logintime as a_logintime,
       b.userid    as b_userid,
       b.logintime as b_logintime
from tb_login a,
     tb_login b;

--过滤数据：用户id相同并且登陆日期相差1
select a_userid,
       a_logintime,
       b_userid,
       b_logintime
from tb_login_tmp
where a_userid = b_userid
  and cast(substr(a_logintime, 9, 2) as int) - 1 = cast(substr(b_logintime, 9, 2) as int);

--统计连续两天登陆用户
select distinct a_userid
from tb_login_tmp
where a_userid = b_userid
  and cast(substr(a_logintime, 9, 2) as int) - 1 = cast(substr(b_logintime, 9, 2) as int);


----窗口函数实现
--连续登陆2天
select userid,
       logintime,
       --本次登陆日期的第二天
       date_add(logintime, 1)                                              as nextday,
       --按照用户id分区，按照登陆日期排序，取下一次登陆时间，取不到就为0
       lead(logintime, 1, 0) over (partition by userid order by logintime) as nextlogin
from tb_login;

--自己实现2次row_number()

select *
from tb_login;
select *,
       row_number() over (partition by userid order by logintime) r
from tb_login;

select t.userid,
       t.logintime,
       t.r,
       date_sub(logintime, r) as lotime
from (select *, row_number() over (partition by userid order by logintime) r
      from tb_login) t;


select t.userid,
       t.logintime,
       t.r,
       date_sub(logintime, r) as lotime
from (select *, row_number() over (partition by userid order by logintime) r
      from tb_login) t;


select userid,
       date_sub(logintime, r),
       count(1) as cnt
from (select *, row_number() over (partition by userid order by logintime) r
      from tb_login) t
group by userid, date_sub(logintime, r)
order by cnt desc
limit 1;


--实现
with t1 as (
    select userid,
           logintime,
           --本次登陆日期的第二天
           date_add(logintime, 1)                                              as nextday,
           --按照用户id分区，按照登陆日期排序，取下一次登陆时间，取不到就为0
           lead(logintime, 1, 0) over (partition by userid order by logintime) as nextlogin
    from tb_login)
select distinct userid
from t1
where nextday = nextlogin;


--连续3天登陆
select userid,
       logintime,
       --本次登陆日期的第三天
       date_add(logintime, 2)                                              as nextday,
       --按照用户id分区，按照登陆日期排序，取下下一次登陆时间，取不到就为0
       lead(logintime, 2, 0) over (partition by userid order by logintime) as nextlogin
from tb_login;

--实现
with t1 as (
    select userid,
           logintime,
           --本次登陆日期的第三天
           date_add(logintime, 2)                                              as nextday,
           --按照用户id分区，按照登陆日期排序，取下下一次登陆时间，取不到就为0
           lead(logintime, 2, 0) over (partition by userid order by logintime) as nextlogin
    from tb_login)
select distinct userid
from t1
where nextday = nextlogin;

--连续N天
select userid,
       logintime,
       --本次登陆日期的第N天
       date_add(logintime, N - 1)                                              as nextday,
       --按照用户id分区，按照登陆日期排序，取下下一次登陆时间，取不到就为0
       lead(logintime, N - 1, 0) over (partition by userid order by logintime) as nextlogin
from tb_login;



--2、级联累加求和
--建表加载数据
create table tb_money
(
    userid string,
    mth    string,
    money  int
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/money.tsv' into table tb_money;

select *
from tb_money;


--	统计得到每个用户每个月的消费总金额
create table tb_money_mtn as
select userid,
       mth,
       sum(money) as m_money
from tb_money
group by userid, mth;

select *
from tb_money_mtn;

--方案一：自连接分组聚合
--	基于每个用户每个月的消费总金额进行自连接
select a.*,
       b.*
from tb_money_mtn a
         join tb_money_mtn b on a.userid = b.userid;

--	将每个月之前月份的数据过滤出来
select a.*,
       b.*
from tb_money_mtn a
         join tb_money_mtn b on a.userid = b.userid
where b.mth <= a.mth;

--	同一个用户 同一个月的数据分到同一组  再根据用户、月份排序
select a.userid,
       a.mth,
       max(a.m_money) as current_mth_money, --当月花费
       sum(b.m_money) as accumulate_money   --累积花费
from tb_money_mtn a
         join tb_money_mtn b on a.userid = b.userid
where b.mth <= a.mth
group by a.userid, a.mth
order by a.userid, a.mth;



--方案二：窗口函数实现
--	统计每个用户每个月消费金额及累计总金额
select userid,
       mth,
       m_money,
       sum(m_money) over (partition by userid order by mth rows between 1 preceding and 2 following) as t_money
from tb_money_mtn;



--3、分组TopN问题
--建表加载数据

create table tb_emp
(
    empno     string,
    ename     string,
    job       string,
    managerid string,
    hiredate  string,
    salary    double,
    bonus     double,
    deptno    string
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/emp.txt' into table tb_emp;

select *
from tb_emp;

select *, row_number() over (partition by deptno order by salary desc) rk
from tb_emp;

with t1 as (select *, row_number() over (partition by deptno order by salary desc) rk from tb_emp)
select t1.empno, t1.deptno, t1.salary
from t1
where rk <= 2;


--	基于row_number实现，按照部门分区，每个部门内部按照薪水降序排序
select empno,
       ename,
       salary,
       deptno,
       row_number() over (partition by deptno order by salary desc) as rn
from tb_emp;

--	过滤每个部门的薪资最高的前两名
with t1 as (
    select empno,
           ename,
           salary,
           deptno,
           row_number() over (partition by deptno order by salary desc) as rn
    from tb_emp)
select *
from t1
where rn < 3;



--------------------------------hive 拉链表设计实现-------------------------------------------------------
--1、建表加载数据
--创建拉链表
create table dw_zipper
(
    userid    string,
    phone     string,
    nick      string,
    gender    int,
    addr      string,
    starttime string,
    endtime   string
) row format delimited fields terminated by '\t';

--加载模拟数据
load data local inpath '/root/hivedata/zipper.txt' into table dw_zipper;
--查询
select userid, nick, addr, starttime, endtime
from dw_zipper;


--	创建ods层增量表 加载数据
create table ods_zipper_update
(
    userid    string,
    phone     string,
    nick      string,
    gender    int,
    addr      string,
    starttime string,
    endtime   string
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/update.txt' into table ods_zipper_update;

select *
from ods_zipper_update;


--合并数据
--创建临时表
create table tmp_zipper
(
    userid    string,
    phone     string,
    nick      string,
    gender    int,
    addr      string,
    starttime string,
    endtime   string
) row format delimited fields terminated by '\t';

--	合并拉链表与增量表
insert overwrite table tmp_zipper
select userid,
       phone,
       nick,
       gender,
       addr,
       starttime,
       endtime
from ods_zipper_update
union all
--查询原来拉链表的所有数据，并将这次需要更新的数据的endTime更改为更新值的startTime
select a.userid,
       a.phone,
       a.nick,
       a.gender,
       a.addr,
       a.starttime,
       --如果这条数据没有更新或者这条数据不是要更改的数据，就保留原来的值，否则就改为新数据的开始时间-1
       if(b.userid is null or a.endtime < '9999-12-31', a.endtime, date_sub(b.starttime, 1)) as endtime
from dw_zipper a
         left join ods_zipper_update b
                   on a.userid = b.userid;


--	覆盖拉链表
insert overwrite table dw_zipper
select *
from tmp_zipper;


insert overwrite table tmp_zipper
select userid,
       phone,
       nick,
       gender,
       addr,
       starttime,
       endtime
from ods_zipper_update
union all
--查询原来拉链表的所有数据，并将这次需要更新的数据的endTime更改为更新值的startTime
select userid,
       phone,
       nick,
       gender,
       addr,
       starttime,
       --如果这条数据没有更新或者这条数据不是要更改的数据，就保留原来的值，否则就改为新数据的开始时间-1
       endtime
from dw_zipper;


select *
from tmp_zipper;


select userid,
       phone,
       nick,
       gender,
       addr,
       starttime,
       lead(date_sub(starttime, 1), 1, '9999-12-31') over (partition by userid order by starttime) as endtime
from tmp_zipper;