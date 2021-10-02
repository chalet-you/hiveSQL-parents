--Hive中事务表的创建使用
--1、开启事务配置（可以使用set设置当前session生效 也可以配置在hive-site.xml中）
set hive.support.concurrency = true; --Hive是否支持并发
set hive.enforce.bucketing = true; --从Hive2.0开始不再需要  是否开启分桶功能
set hive.exec.dynamic.partition.mode = nonstrict; --动态分区模式  非严格
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; --
set hive.compactor.initiator.on = true; --是否在Metastore实例上运行启动压缩合并
set hive.compactor.worker.threads = 1; --在此metastore实例上运行多少个压缩程序工作线程。


--事务表的创建
CREATE TABLE emp (id int, name string, salary int)
STORED AS ORC TBLPROPERTIES ('transactional' = 'true');

--事务表 insert  -->delta文件
INSERT INTO emp VALUES
(1, 'Jerry', 5000),
(2, 'Tom',   8000),
(3, 'Kate',  6000);

select * from emp;

--再次insert  --->delta文件
INSERT INTO emp VALUES(4, 'Allen', 8000);

--执行delete --> delete-delta文件
delete from emp where id =2;

--显示有关当前运行的压缩和最近的压缩历史
Show Compactions;




--2、创建Hive事务表
create table trans_student(
                              id int,
                              name String,
                              age int
)stored as orc TBLPROPERTIES('transactional'='true');

describe formatted trans_student;

--3、针对事务表进行insert update delete操作
insert into trans_student (id, name, age)
values (1,"allen",18);

select *
from trans_student;

describe formatted trans_student;

update trans_student
set age = 20
where id = 1;

delete from trans_student where id =1;

select *
from trans_student;


show tables;

select * from student_local;

update student_local
set  age= 35
where num =95001;