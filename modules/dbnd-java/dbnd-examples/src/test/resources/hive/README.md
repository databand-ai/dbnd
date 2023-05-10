# Docker-compose for Hive testing

1. Run `docker-compose up` to spin up all Hive-related machinery.
2. Run `docker exec -it hive-server /bin/bash` to connect to the Hive server
3. Run `hive -f /data/employees_table.hql`
4. Run `hadoop fs -put /data/employees.csv hdfs://namenode:8020/user/hive/warehouse/testdb.db/employees`
5. Run `hive -f /data/stores_table.hql`
6. Run `hadoop fs -put /data/stores.csv hdfs://namenode:8020/user/hive/warehouse/testdb.db/stores`
7. Uncomment Hive-related things in `build.gradle`
8. Build jars with `./gradlew fatJar`
9. Connect to the spark server `docker exec -it spark /bin/bash`
10. Run app using script `submit.sh`

