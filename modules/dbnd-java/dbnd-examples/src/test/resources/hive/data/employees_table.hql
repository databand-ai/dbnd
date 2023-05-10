create database if not exists testdb;
use testdb;
create external table if not exists employees
(
    eid           int,
    ename         string,
    age           int,
    jobtype       string,
    storeid       int,
    storelocation string,
    salary        bigint,
    yrsofexp      int
)
    row format delimited
        fields terminated by ','
        lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/testdb.db/employees';

insert overwrite table testdb.stores_stats
select stores.storename, count(*)
from testdb.stores stores
         join testdb.employees employee on stores.storeid = employee.storeid
group by storename;
