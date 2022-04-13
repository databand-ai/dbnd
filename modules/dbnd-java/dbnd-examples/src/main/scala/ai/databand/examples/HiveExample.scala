package ai.databand.examples

import ai.databand.spark.DbndSparkQueryExecutionListener
import org.apache.spark.sql.SparkSession

object HiveExample {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder.appName("process_data_scala_spark")
            .master("local[*]")
            .config("hive.metastore.uris", "thrift://localhost:9083")
            .config("fs.s3.impl", "com.amazon.ws.emr.hadoop.fs.EmrFileSystem")
            .config("fs.s3.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
            .config("fs.s3.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
            .config("fs.s3.endpoint", "s3.us-east-1.amazonaws.com")
            .enableHiveSupport()
            .getOrCreate

        spark.listenerManager.register(new DbndSparkQueryExecutionListener())

        val df = spark.sql("SELECT * FROM impressions where dt = '2009-04-13-08-05'")
        df.show(45)

        spark.stop()
    }
}
