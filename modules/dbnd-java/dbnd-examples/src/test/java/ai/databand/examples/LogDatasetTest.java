package ai.databand.examples;

import ai.databand.annotations.Task;
import ai.databand.log.DbndLogger;
import ai.databand.log.LogDatasetRequest;
import ai.databand.schema.DatasetOperationRes;
import ai.databand.schema.Job;
import ai.databand.schema.Pair;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.TaskRun;
import ai.databand.schema.Tasks;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static ai.databand.schema.DatasetOperationStatus.OK;
import static ai.databand.schema.DatasetOperationType.READ;

/**
 * This test checks log_dataset_op and descriptive stats calculation.
 */
public class LogDatasetTest {

    private static class LogDatasetPipeline {

        @Task("log_dataset_pipeline")
        public static void main(String[] args) {
            SparkSession spark = SparkSession.builder()
                .appName("Log Dataset Pipeline")
                .master("local[*]")
                .getOrCreate();

            SQLContext sql = spark.sqlContext();
            String path = args.length > 0 ? args[0] : LogDatasetPipeline.class.getClassLoader().getResource("usa-education-budget.csv").getFile();
            Dataset<?> data = sql.read().option("inferSchema", "true").option("header", "true").csv(path);
            DbndLogger.logDatasetOperation("s3://databand/samples/usa-education-budget.csv", READ, OK, data, new LogDatasetRequest().withPreview().withSchema());
            spark.stop();
        }

    }

    private static PipelinesVerify pipelinesVerify;

    @BeforeAll
    static void beforeAll() throws Exception {
        pipelinesVerify = new PipelinesVerify(new ApiWithTokenBuilder().api());
    }

    /**
     * Scala pipeline test uses auto-inject listener.
     *
     * @throws IOException
     */
    @Test
    public void testScalaPipeline() throws IOException {
        ZonedDateTime now = ZonedDateTime.now().minusSeconds(1L);
        LogDatasetPipeline.main(new String[]{});

        String jobName = "log_dataset_pipeline";
        Job job = pipelinesVerify.verifyJob(jobName);
        Pair<Tasks, TaskFullGraph> tasksAndGraph = pipelinesVerify.verifyTasks(jobName, job);
        TaskFullGraph graph = tasksAndGraph.right();
        Tasks tasks = tasksAndGraph.left();

        TaskRun main = pipelinesVerify.assertTaskExists(jobName + "-parent", tasks, "success");
        Map<String, List<DatasetOperationRes>> datasetOpsByTask = pipelinesVerify.fetchDatasetOperations(job);

        DatasetOperationRes op = pipelinesVerify.assertDatasetOperationExists(
            "log_dataset_pipeline",
            "s3://databand/samples/usa-education-budget.csv",
            READ,
            "SUCCESS",
            41,
            1,
            datasetOpsByTask
        );

        pipelinesVerify.assertColumnStat(
            op.getColumnsStats(),
            "RATIO",
            "double",
            41,
            39,
            0.44597560,
            0.237,
            0.915,
            0.1158023,
            0.383,
            0.421,
            0.496
        );


        pipelinesVerify.assertColumnStat(
            op.getColumnsStats(),
            "BUDGET_ON_EDUCATION",
            "double",
            41,
            41,
            40138.12195121951,
            9314.0,
            131891.0,
            27530.137729582035,
            18952.0,
            31816.0,
            57458.0
        );
    }
}
