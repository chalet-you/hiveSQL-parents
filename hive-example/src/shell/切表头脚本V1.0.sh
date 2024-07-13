#!/bin/bash
# 检查columns空值
# 解析参数
while getopts "r:t:d:c:s:x:l:" arg; do
  case $arg in
  # 质检编码
  r)
    RULE=$OPTARG
    ;;
  # 要处理的表名
  t)
    TABLE=$OPTARG
    ;;
  # 日期
  d)
    DT=$OPTARG
    ;;
  # 要计算空值的列名
  c)
    COL=$OPTARG
    ;;
  # 空值指标下限
  s)
    MIN=$OPTARG
    ;;
  # 空值指标上限
  x)
    MAX=$OPTARG
    ;;
  # 告警级别
  l)
    LEVEL=$OPTARG
    ;;
  ?)
    echo "unkonw argument"
    exit 1
    ;;
  esac
done

#如果dt和level没有设置，那么默认值dt是上个月 告警级别是0
[ "$DT" ] || DT=$(date -d '1 month ago' +%Y%m)
[ "$LEVEL" ] || LEVEL=0



# MySQL相关配置
mysql_user="root"
mysql_passwd="000000"
mysql_host="hadoop102"
mysql_port="3306"
mysql_DB="test"
mysql_tbl="null_columns"



# 空值个数
hive_query="set hive.cli.print.header=false;select count(if($COL is null or trim($COL) = '',1,null)), count(1) from $TABLE where version <= '$DT';"
hive_query="select count(if($COL is null or trim($COL) = '',1,null)), count(1) from $TABLE;"

# 执行 Hive 查询，并将结果存储到变量中
RESULT=$(beeline -u jdbc:hive2://hadoop102:10000 -n atguigu -p 123456 -e "$hive_query"| tail -n +4|head -n -1 |sed 's/|//g'|sed 's/^[[:space:]]\{1,\}//'|sed 's/[[:space:]]*$//')

echo "$RESULT"

# 构造插入MySQL语句
insert_stmt="insert into $mysql_DB.$mysql_tbl (rule_code, dt, tbl, col, value, total, value_min, value_max, notification_level) VALUES"

# 循环处理查询结果，构造插入语句
while IFS=$' +' read -r NUM CNT; do
    insert_stmt+=" ('$RULE', '$DT', '$TABLE', '$COL', $NUM, $CNT, $MIN, $MAX, $LEVEL),"
done <<< "$RESULT"


# 移除最后一个逗号
insert_stmt=${insert_stmt%,}

# 构造幂等性插入MySQL语句
insert_stmt+=" ON DUPLICATE KEY UPDATE value=VALUES(value), total=VALUES(total), value_min=VALUES(value_min), value_max=VALUES(value_max), notification_level=VALUES(notification_level);"
echo "${insert_stmt}"

# 执行插入
mysql -h"$mysql_host" -P"$mysql_port" -u"$mysql_user" -p"$mysql_passwd" -e "$insert_stmt"

