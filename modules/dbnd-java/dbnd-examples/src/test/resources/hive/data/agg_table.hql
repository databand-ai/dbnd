create database if not exists testdb;
use testdb;
create external table if not exists stores_stats (
                                                 storename string,
                                                 employees_count int
)
    row format delimited
        fields terminated by ','
        lines terminated by '\n'
    stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/testdb.db/stores_stats';
