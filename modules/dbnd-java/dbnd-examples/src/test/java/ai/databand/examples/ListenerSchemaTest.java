/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.examples;

import ai.databand.annotations.Task;
import ai.databand.log.DbndLogger;
import ai.databand.schema.DatasetOperationRes;
import ai.databand.schema.DatasetOperationType;
import ai.databand.schema.Job;
import ai.databand.schema.LogDataset;
import ai.databand.schema.Pair;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.TaskRun;
import ai.databand.schema.Tasks;
import ai.databand.spark.DbndSparkQueryExecutionListener;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This test checks log_dataset_op from listener when performed spark operation does not require schema.
 * For operations like `count` Spark Listener won't receive schema in the FileSourceScanExec node.
 * Schema should be extracted from the relation.
 */
public class ListenerSchemaTest {

    private static class TestSchemaPipeline {

        @Task("test_schema_pipeline")
        public static void main(String[] args) {
            SparkSession spark = SparkSession.builder()
                .appName("Log Dataset Pipeline")
                .config("spark.sql.shuffle.partitions", 1)
                .master("local[*]")
                .getOrCreate();

            spark.listenerManager().register(new DbndSparkQueryExecutionListener());

            SQLContext sql = spark.sqlContext();

            String path = args.length > 0 ? args[0] : TestSchemaPipeline.class.getClassLoader().getResource("usa-education-budget.csv").getFile();
            Dataset<?> data = sql.read().option("inferSchema", "true").option("header", "true").csv(path);

            DbndLogger.logMetric("total_records", data.count());
            spark.stop();
        }
    }

    @Test
    public void testSchema() throws IOException {
        PipelinesVerify pipelinesVerify = new PipelinesVerify();
        TestSchemaPipeline.main(new String[]{});

        String jobName = "test_schema_pipeline";
        Job job = pipelinesVerify.verifyJob(jobName);
        Pair<Tasks, TaskFullGraph> tasksAndGraph = pipelinesVerify.verifyTasks(jobName, job);
        Tasks tasks = tasksAndGraph.left();

        TaskRun taskRun = pipelinesVerify.assertTaskExists(jobName, tasks, "success");
        Map<String, List<DatasetOperationRes>> datasetOpsByTask = pipelinesVerify.fetchDatasetOperations(job);

        pipelinesVerify.assertMetricInTask(taskRun, "total_records", 41, "user");
        DatasetOperationRes op = pipelinesVerify.assertDatasetOperationExists(
            "test_schema_pipeline",
            "build/resources/test",
            DatasetOperationType.READ,
            "SUCCESS",
            41,
            1,
            datasetOpsByTask,
            null,
            LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
        );

        pipelinesVerify.assertListenerColumnStat(op.getColumnsStats(), "RATIO", "double", 41);
        pipelinesVerify.assertListenerColumnStat(op.getColumnsStats(), "YEAR", "integer", 41);
        pipelinesVerify.assertListenerColumnStat(op.getColumnsStats(), "GDP", "double", 41);
        pipelinesVerify.assertListenerColumnStat(op.getColumnsStats(), "RATIO", "double", 41);
    }
}
