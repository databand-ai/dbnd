package ai.databand.examples

import ai.databand.spark.DbndSparkQueryExecutionListener
import org.apache.spark.sql.SparkSession

object HiveLocalExample {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder.appName("hive_local_example")
            .master("local[*]")
            .config("hive.metastore.uris", "thrift://hive-metastore:9083")
            .enableHiveSupport()
            .getOrCreate

        spark.listenerManager.register(new DbndSparkQueryExecutionListener())

        countEmployees(spark)
        insertStores(spark)

        spark.stop()
    }

    def countEmployees(spark: SparkSession): Unit = {
        val query = "select count(*) from testdb.employee"
        spark.sql(query).show()
    }

    def insertStores(spark: SparkSession): Unit = {
        val query = "insert overwrite table testdb.stores_stats\n" +
            "select stores.storename, count(*)\n" +
            "from testdb.stores stores\n" +
            "         join testdb.employee employee on stores.storeid = employee.storeid\n" +
            "group by storename"

        spark.sql(query)
    }
}
