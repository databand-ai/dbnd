package ai.databand.examples

import ai.databand.annotations.Task
import ai.databand.spark.DbndSparkQueryExecutionListener
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object JoinPipeline {

    private val LOG = LoggerFactory.getLogger(ProcessDataSparkScala.getClass)

    @Task("join_pipeline")
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("JoinPipeline")
            .config("spark.sql.shuffle.partitions", 1)
            .master("local[*]")
            .getOrCreate
        spark.listenerManager.register(new DbndSparkQueryExecutionListener())

        val customersPath = JoinPipeline.getClass.getClassLoader.getResource("customers.csv").getFile()
        val ordersPath = JoinPipeline.getClass.getClassLoader.getResource("orders.csv").getFile()
        val output = System.getenv("PROCESS_DATA_OUTPUT") + "/join_pipeline"

        // Read customer and orders data
        val ordersDF = spark.read
            .option("header", true)
            .option("inferSchema", true)
            .csv(ordersPath)

        val customersDF = spark.read
            .option("header", true)
            .option("inferSchema", true)
            .csv(customersPath)

        // Join on the customer key
        val fullDF = ordersDF.join(customersDF, ordersDF("customer_id") === customersDF("customer_id"), "left")

        // Clean up the column names
        val cleanDF = fullDF.select(
            col("order_id").as("Order ID"),
            col("order_date").as("Order Date"),
            col("total_price").as("Order Total"),
            col("name").as("Customer Name"),
            col("market_segment").as("Market Segment")
        )

        // Get order totals by market segment for urgent orders
        val customerTotalsDF = cleanDF
            .groupBy("Market Segment").agg(sum("Order Total").as("Market Segment Total"))
            .withColumn("Market Segment Total", format_number(col("Market Segment Total"), 2))
            .orderBy(col("Market Segment Total").desc)

        // Write the market segment totals to CSV
        customerTotalsDF.write.mode("overwrite").csv(output)
    }

}
