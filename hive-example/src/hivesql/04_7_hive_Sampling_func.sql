--数据表
select * from student;

--需求：随机抽取2个学生的情况进行查看
SELECT * FROM student
DISTRIBUTE BY rand() SORT BY rand() LIMIT 2;

--使用order by+rand也可以实现同样的效果 但是效率不高
SELECT * FROM student
    ORDER BY rand() LIMIT 2;


---block抽样
--根据行数抽样
SELECT * FROM student TABLESAMPLE(1 ROWS);

--根据数据大小百分比抽样
select * from db_itheima.student tablesample ( 50 percent );



--根据数据大小抽样
--支持数据单位 b/B, k/K, m/M, g/G
SELECT * FROM student TABLESAMPLE(1k);


---bucket table抽样
--根据整行数据进行抽样
SELECT * FROM t_usa_covid19_bucket TABLESAMPLE(BUCKET 1 OUT OF 5 ON rand());

--根据分桶字段进行抽样 效率更高
describe formatted t_usa_covid19_bucket;
SELECT * FROM t_usa_covid19_bucket TABLESAMPLE(BUCKET 1 OUT OF 5 ON state);


--TABLESAMPLE (BUCKET x OUT OF y [ON colname])

--1、y必须是table总bucket数的倍数或者因子。hive根据y的大小，决定抽样的比例。
    --例如，table总共分了4份（4个bucket），当y=2时，抽取(4/2=)2个bucket的数据，当y=8时，抽取(4/8=)1/2个bucket的数据。
--2、x表示从哪个bucket开始抽取。
    --例如，table总bucket数为4，tablesample(bucket 4 out of 4)，表示总共抽取（4/4=）1个bucket的数据，抽取第4个bucket的数据。
    --注意：x的值必须小于等于y的值，否则FAILED:Numerator should not be bigger than denominator in sample clause for table stu_buck
--3、ON colname表示基于什么抽
    --ON rand()表示随机抽
    --ON 分桶字段 表示基于分桶字段抽样 效率更高 推荐