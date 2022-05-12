package ai.databand.demo

import org.apache.spark.sql.SparkSession

object MainSdkOnly {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder.appName("process_data_scala_spark_sdk_only").master("local[*]").getOrCreate
        val processor = new ProcessDataSparkScalaSdkOnly(spark)
        processor.processCustomerData(args(0), args(1))
        spark.stop()
    }
}
