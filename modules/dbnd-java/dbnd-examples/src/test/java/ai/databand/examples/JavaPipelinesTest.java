/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
 */

package ai.databand.examples;

import ai.databand.spark.DbndSparkQueryExecutionListener;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;

class JavaPipelinesTest {

    @BeforeAll
    static void setup() {
        if(!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            BasicConfigurator.configure();
        }
        Logger.getLogger("ai.databand").setLevel(Level.DEBUG);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.spark_project").setLevel(Level.WARN);
        Logger.getLogger("io.netty").setLevel(Level.INFO);
    }

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
