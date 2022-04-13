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
    static void beforeAll() throws Exception {
        pipelinesVerify = new PipelinesVerify(new ApiWithTokenBuilder().api());
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
            .master("local[*]")
            .getOrCreate();

        spark.sparkContext().addSparkListener(new DbndSparkListener());

        JavaSparkPipeline javaSparkPipeline = new JavaSparkPipeline(spark);
        String file = this.getClass().getClassLoader().getResource("sample.json").getFile();
        javaSparkPipeline.execute(file);

        pipelinesVerify.verifyOutputs("spark_java_pipeline", now, "spark_java_pipeline");

        spark.stop();
    }

    /**
     * This pipeline does not inject listener automatically and manually.
     *
     * @throws IOException
     */
    @Test
    public void testBadJavaPipeline() throws IOException {
        ZonedDateTime now = ZonedDateTime.now();
        SparkSession spark = SparkSession
            .builder()
            .appName("DBND Spark Java Bad Pipeline")
            .master("local[*]")
            .getOrCreate();

        JavaSparkPipeline javaSparkPipeline = new JavaSparkPipeline(spark);
        Assertions.assertThrows(RuntimeException.class, () -> javaSparkPipeline.executeBad("error"));

        spark.stop();

        String jobName = "spark_java_bad_pipeline";
        Job job = pipelinesVerify.verifyJob(jobName);
        Pair<Tasks, TaskFullGraph> tasksAndGraph = pipelinesVerify.verifyTasks(jobName, job);
        TaskFullGraph graph = tasksAndGraph.right();
        Tasks tasks = tasksAndGraph.left();

        TaskRun executeBad = pipelinesVerify.assertTaskExists("spark_java_bad_pipeline-parent", tasks, "failed");
        assertThat(
            String.format("Last run was created before the test run, thus, it's not the run we're looking for. Now: %s, run start date: %s",
                now, executeBad.getStartDate()),
            executeBad.getEndDate().isAfter(now),
            Matchers.is(true)
        );

        Map<String, Integer> tasksAttemptsIds = tasks.getTaskInstances().values()
            .stream()
            .collect(Collectors.toMap(TaskRun::getUid, TaskRun::getLatestTaskRunAttemptId));

        pipelinesVerify.assertLogsInTask(
            tasksAttemptsIds.get(executeBad.getUid()),
            "java.lang.RuntimeException: Unable to complete the pipeline"
        );

        pipelinesVerify.assertTaskExists("loadTracks", tasks, "success");
    }
}
