
-----------------------1、Hive表设计优化-----------------------------------
--a、分区表优化
--创建数据库
create database tb_part;

--创建表
create table tb_login(
                         userid string,
                         logindate string
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/login.log' into table tb_login;

select * from tb_login;

-- 统计3月24号的登录人数
select
    logindate,
    count(*) as cnt
from tb_login
where logindate = '2021-03-24'
group by logindate;

--执行计划
explain extended
select
    logindate,
    count(*) as cnt
from tb_login
where logindate = '2021-03-24'
group by logindate;

--创建分区表 按照登录日期分区
create table tb_login_part(
    userid string
)
    partitioned by (logindate string)
    row format delimited fields terminated by '\t';

--开启动态分区
set hive.exec.dynamic.partition.mode=nonstrict;
--按登录日期分区
insert into table tb_login_part partition(logindate)
select * from tb_login;

--基于分区表查询数据
explain extended
select
    logindate,
    count(*) as cnt
from tb_login_part
where logindate = '2021-03-23' or logindate = '2021-03-24'
group by logindate;


--b、分桶表优化
--	构建普通emp表
use db_itheima;
--创建普通表
create table tb_emp01(
                         empno string,
                         ename string,
                         job string,
                         managerid string,
                         hiredate string,
                         salary double,
                         jiangjin double,
                         deptno string
) row format delimited fields terminated by '\t';
--加载数据
load data local inpath '/root/hivedata/emp01.txt' into table tb_emp01;

select * from tb_emp01;

--	构建分桶emp表
-- 创建分桶表
create table tb_emp02(
                         empno string,
                         ename string,
                         job string,
                         managerid string,
                         hiredate string,
                         salary double,
                         jiangjin double,
                         deptno string
)
    clustered by(deptno) sorted by (deptno asc) into 3 buckets
    row format delimited fields terminated by '\t';
-- 写入分桶表
insert overwrite table tb_emp02
select * from tb_emp01;

--	构建普通dept表
create table tb_dept01(
                          deptno string,
                          dname string,
                          loc string
)
    row format delimited fields terminated by ',';
-- 加载数据
load data local inpath '/root/hivedata/dept01.txt' into table tb_dept01;

select * from tb_dept01;


--	构建分桶dept表
create table tb_dept02(
                          deptno string,
                          dname string,
                          loc string
)
    clustered by(deptno) sorted by (deptno asc) into 3 buckets
    row format delimited fields terminated by ',';
--写入分桶表
insert overwrite table tb_dept02
select * from tb_dept01;


--普通join的执行优化
explain
select
    a.empno,
    a.ename,
    a.salary,
    b.deptno,
    b.dname
from tb_emp01 a join tb_dept01 b on a.deptno = b.deptno;

--	分桶的Join执行计划
--开启分桶SMB(Sort-Merge-Buket) join
set hive.optimize.bucketmapjoin = true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
--查看执行计划
explain
select
    a.empno,
    a.ename,
    a.salary,
    b.deptno,
    b.dname
from tb_emp02 a join tb_dept02 b on a.deptno = b.deptno;




-----------------------2、Hive表数据优化-----------------------------------
--a、文件格式
-- 创建原始数据表
create table tb_sogou_source(
                                stime string,
                                userid string,
                                keyword string,
                                clickorder string,
                                url string
)
    row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/SogouQ.reduced' into table tb_sogou_source;

--创建textfile数据表
create table tb_sogou_text(
                              stime string,
                              userid string,
                              keyword string,
                              clickorder string,
                              url string
)
    row format delimited fields terminated by '\t'
    stored as textfile;
--	写入TextFile数据表
insert into table tb_sogou_text
select * from tb_sogou_source;

--sequencefile
create table tb_sogou_seq(
                             stime string,
                             userid string,
                             keyword string,
                             clickorder string,
                             url string
)
    row format delimited fields terminated by '\t'
    stored as sequencefile;

insert into table tb_sogou_seq
select * from tb_sogou_source;

--Parquet格式
create table tb_sogou_parquet(
                                 stime string,
                                 userid string,
                                 keyword string,
                                 clickorder string,
                                 url string
)
    row format delimited fields terminated by '\t'
    stored as parquet;

insert into table tb_sogou_parquet
select * from tb_sogou_source;

--ORC格式
create table tb_sogou_orc(
                             stime string,
                             userid string,
                             keyword string,
                             clickorder string,
                             url string
)
    row format delimited fields terminated by '\t'
    stored as orc;

insert into table tb_sogou_orc
select * from tb_sogou_source;

--b、数据压缩
--开启hive中间传输数据压缩功能
--1）开启hive中间传输数据压缩功能
set hive.exec.compress.intermediate=true;
--2）开启mapreduce中map输出压缩功能
set mapreduce.map.output.compress=true;
--3）设置mapreduce中map输出数据的压缩方式
set mapreduce.map.output.compress.codec= org.apache.hadoop.io.compress.SnappyCodec;


--开启Reduce输出阶段压缩
--1）开启hive最终输出数据压缩功能
set hive.exec.compress.output=true;
--2）开启mapreduce最终输出数据压缩
set mapreduce.output.fileoutputformat.compress=true;
--3）设置mapreduce最终数据输出压缩方式
set mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;
--4）设置mapreduce最终数据输出压缩为块压缩
set mapreduce.output.fileoutputformat.compress.type=BLOCK;


--创建表，指定为textfile格式，并使用snappy压缩
drop table tb_sogou_snappy;
create table tb_sogou_snappy
    stored as textfile
as select * from tb_sogou_source;

--创建表，指定为orc格式，并使用snappy压缩
create table tb_sogou_orc_snappy
    stored as orc tblproperties ("orc.compress"="SNAPPY")
as select * from tb_sogou_source;



---存储优化-ORC文件索引
--1、开启索引配置
set hive.optimize.index.filter=true;
--2、创建表并制定构建索引
create table tb_sogou_orc_index
    stored as orc tblproperties ("orc.create.index"="true")
as select * from tb_sogou_source
    distribute by stime
    sort by stime;
--3、当进行范围或者等值查询（<,>,=）时就可以基于构建的索引进行查询
select count(*) from tb_sogou_orc_index where stime > '12:00:00' and stime < '18:00:00';


--创建表指定创建布隆索引
create table tb_sogou_orc_bloom
stored as orc tblproperties ("orc.create.index"="true","orc.bloom.filter.columns"="stime,userid")
as select * from tb_sogou_source
distribute by stime
sort by stime;

--stime的范围过滤可以走row group index，userid的过滤可以走bloom filter index
select
    count(*)
from tb_sogou_orc_index
where stime > '12:00:00' and stime < '18:00:00'
  and userid = '3933365481995287' ;



-----------------------3、Hive Job作业执行优化-----------------------------------

--a、explain执行计划
explain extended select * from tb_emp;

explain select count(*) as cnt from tb_emp where deptno = '10';


--
select a.id,a.value1,b.value2 from table1 a
join (select b.* from table2 b where b.ds>='20181201' and b.ds<'20190101') c
on (a.id=c.id)

select a.id,a.value1,b.value2 from table1 a
join table2 b on a.id=b.id
where b.ds>='20181201' and b.ds<'20190101'

-----------------------4、Hive3新特性-----------------------------------