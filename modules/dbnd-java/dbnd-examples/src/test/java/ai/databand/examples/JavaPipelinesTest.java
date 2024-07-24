/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.examples;

import ai.databand.spark.DbndSparkQueryExecutionListener;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;

class JavaPipelinesTest {

    @Test
    public void testJavaPipeline() throws IOException {
        ZonedDateTime now = ZonedDateTime.now();
        SparkSession spark = SparkSession
            .builder()
            .appName("DBND Spark Java Pipeline")
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.env.DBND__LOG__PREVIEW_HEAD_BYTES",32 * 1024)
            .config("spark.env.DBND__LOG__PREVIEW_TAIL_BYTES",32 * 1024)
            .master("local[*]")
            .getOrCreate();

        spark.listenerManager().register(new DbndSparkQueryExecutionListener());

        JavaSparkPipeline javaSparkPipeline = new JavaSparkPipeline(spark);
        String input = getClass().getClassLoader().getResource("p_a_master_data.csv").getFile();
        String output = System.getenv("PROCESS_DATA_OUTPUT") + "/java_pipeline";
        javaSparkPipeline.main(new String[]{input, output});

        new PipelinesVerify().verifyOutputs("java_spark_pipeline", now, "java_spark_pipeline");

        spark.stop();
    }

}
