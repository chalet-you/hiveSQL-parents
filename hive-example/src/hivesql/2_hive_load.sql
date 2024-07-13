use db_itheima;
create table student_local
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
)
    row format delimited fields terminated by ',';
load data local inpath '/root/hivedata/students.txt' into table student_local;



create external table student_HDFS
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
) row format delimited fields terminated by ',';



create external table student_HDFS_P
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
)
partitioned by (country string)
row format delimited fields terminated by ',';

create table if not exists tab1(
    col1 int,
    col2 int
)
partitioned by (col3 int)
row format delimited fields terminated by ',';

load data local inpath '/root/hivedata/tab1.txt' into table tab1 partition (col3="2");

