package ai.databand.examples;

import ai.databand.schema.Job;
import ai.databand.schema.Pair;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.TaskRun;
import ai.databand.schema.Tasks;
import ai.databand.spark.DbndSparkListener;
import org.apache.spark.sql.SparkSession;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;

class JavaPipelinesTest {

    private static PipelinesVerify pipelinesVerify;

    @BeforeAll
    static void beforeAll() throws IOException {
        pipelinesVerify = new PipelinesVerify();
    }

    /**
     * Java pipeline uses manual listener inject.
     *
     * @throws IOException
     */
    @Test
    public void testJavaPipeline() throws IOException {
        ZonedDateTime now = ZonedDateTime.now();
        SparkSession spark = SparkSession
            .builder()
            .appName("DBND Spark Java Pipeline")
            .config("spark.sql.shuffle.partitions", 1)
            .master("local[*]")
            .getOrCreate();

        spark.sparkContext().addSparkListener(new DbndSparkListener());

        JavaSparkPipeline javaSparkPipeline = new JavaSparkPipeline(spark);
        String file = this.getClass().getClassLoader().getResource("sample.json").getFile();
        javaSparkPipeline.execute(file);

        pipelinesVerify.verifyOutputs("spark_java_pipeline", now, "spark_java_pipeline");

        spark.stop();
    }

}
