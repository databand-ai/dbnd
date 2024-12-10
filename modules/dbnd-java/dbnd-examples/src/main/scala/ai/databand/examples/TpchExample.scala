/*
 * © Copyright Databand.ai, an IBM Company 2024
 */

package ai.databand.examples

import ai.databand.annotations.Task
import ai.databand.spark.DbndSparkQueryExecutionListener
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object TpchExample {

    private val LOG = LoggerFactory.getLogger(ProcessDataSparkScala.getClass)

    @Task("tpch_example")
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("TpchExample")
            .config("spark.sql.shuffle.partitions", 1)
            .master("local[*]")
            .getOrCreate
        spark.listenerManager.register(new DbndSparkQueryExecutionListener())

        val lineitemPath = TpchExample.getClass.getClassLoader.getResource("lineitem.csv").getFile()
        val partPath = TpchExample.getClass.getClassLoader.getResource("part.csv").getFile()
        val output = System.getenv("PROCESS_DATA_OUTPUT") + "/tpch_example"

        // Read lineitem and part data
        val lineitemDF = spark.read
            .option("header", true)
            .option("inferSchema", true)
            .csv(lineitemPath)

        val partDF = spark.read
            .option("header", true)
            .option("inferSchema", true)
            .csv(partPath)

        val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

        // Sample tpch-inspired query, which uses "org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec" Spark nodes in its execution plan
        val revenueDF = partDF.join(lineitemDF, col("l_partkey") === col("p_partkey"))
            .filter((col("l_shipmode") === "AIR" || col("l_shipmode") === "AIR REG") && col("l_shipinstruct") === "DELIVER IN PERSON")
            .select(decrease(col("l_extendedprice"), col("l_discount")).as("finalPrice"))
            .agg(sum("finalPrice"))

        revenueDF.write.mode("overwrite").csv(output)
    }

}
