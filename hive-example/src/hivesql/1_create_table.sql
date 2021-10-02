show databases;

-- 切换数据库
use db_itheima;
create table t_archer(
    id              int         comment "ID",
    name            string      comment "英雄生命",
    hp_max          int         comment "最大生命",
    mp_max          int         comment "最大法力",
    attack_max      int         comment "最高物攻",
    defense_max     int         comment "最大物防",
    attack_range    string      comment "攻击范围",
    role_main       string      comment "主要定位",
    role_assist     string      comment "决要定位"
) comment "王者荣耀射手信息"
row format delimited fields terminated by "\t";

select * from t_archer;

-- 复杂数据类型
create table t_hot_hero_skin_price(
    id int,
    name string,
    win_rate int,
    skin_price map<string,int> -- 注意map复合类型
)
row format delimited
fields terminated by ',' -- 指定字段分隔符
collection items terminated by '-' --指定集合元素之间的分隔符
map keys terminated by ':' ;-- 指定map元素kv之间的分隔符

select * from t_hot_hero_skin_price;

create table if not exists t_team_ace_player_localtion1(
    id int,
    team_name string,
    ace_player_name string
)
location '/datas/';
select *
from t_team_ace_player_localtion1;

desc formatted t_team_ace_player_localtion1;

create table student(
                        num int,
                        name string,
                        sex string,
                        age int,
                        dept string)
    row format delimited
        fields terminated by ',';
load data local inpath '/export/data/student.txt' into table student;


create external table exteral_student(
                        num int,
                        name string,
                        sex string,
                        age int,
                        dept string)
    row format delimited
        fields terminated by ',';
load data local inpath '/export/data/student.txt' into table exteral_student;

drop table student;
drop table exteral_student;


CREATE TABLE db_itheima.t_usa_covid19_bucket(
                                             count_date string,
                                             county string,
                                             state string,
                                             fips int,
                                             cases int,
                                             deaths int)
                                             clustered by (state) into 5 buckets ;


create table db_itheima.t_usa_covid19(
                                         count_date string,
                                         county string,
                                         state string,
                                         fips int,
                                         cases int,
                                         deaths int)
                                         row format delimited fields terminated by ',';
load data local inpath '/export/data/us-covid19-counties.dat' into table t_usa_covid19;



insert into db_itheima.t_usa_covid19_bucket select * from t_usa_covid19 cluster by (state);
insert into db_itheima.t_usa_covid19_bucket select * from t_usa_covid19;















