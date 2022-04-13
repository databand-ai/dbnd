package ai.databand.benchmarks.columns

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{NumericType, StringType}

object SparkManualStats {

    def main(args: Array[String]): Unit = {
        val dataPath = args(0)

        val spark = SparkSession.builder.appName("SparkStats")
            .master("spark://127.0.0.1:7077")
            .getOrCreate

        val df = spark.read
            .option("inferSchema", "True")
            .option("header", "True")
            .csv(dataPath)
            .select("capacity_bytes", "smart_1_raw", "smart_5_raw", "smart_9_raw", "smart_194_raw", "smart_197_raw")

        val expr = df.schema.fields.flatMap {
            field => {
                field.dataType match {
                    case _: NumericType => Seq(count(field.name), max(field.name), min(field.name), stddev(field.name), mean(field.name))
                    case _: StringType => Seq(count(field.name), min(field.name), max(field.name))
                }
            }
        }

        df.select(expr: _*).show()
    }

}
