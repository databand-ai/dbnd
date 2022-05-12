package ai.databand.demo

import org.apache.spark.sql.SparkSession

object Main {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder.appName("process_data_scala_spark").master("local[*]").getOrCreate
        val processor = new ProcessDataSparkScala(spark)
        processor.processCustomerData(args(0), args(1))
        spark.stop()
    }
}
