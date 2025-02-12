/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
 */

package ai.databand.examples;

import ai.databand.annotations.Task;
import ai.databand.log.DbndLogger;
import ai.databand.log.LogDatasetRequest;
import ai.databand.schema.DatasetOperationRes;
import ai.databand.schema.DatasetOperationType;
import ai.databand.schema.Job;
import ai.databand.schema.LogDataset;
import ai.databand.schema.Pair;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.Tasks;
import ai.databand.spark.DbndSparkQueryExecutionListener;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static ai.databand.schema.DatasetOperationStatus.NOK;
import static ai.databand.schema.DatasetOperationStatus.OK;
import static ai.databand.schema.DatasetOperationType.READ;
import static ai.databand.schema.DatasetOperationType.WRITE;

/**
 * This test checks log_dataset_op and descriptive stats calculation.
 */
public class LogDatasetTest {

    @BeforeAll
    static void setup() {
        if(!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            BasicConfigurator.configure();
        }

        Logger.getLogger("ai.databand").setLevel(Level.INFO);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.spark_project").setLevel(Level.WARN);
    }

    private static class LogDatasetPipeline {

        @Task("log_dataset_pipeline")
        public static void main(String[] args) {
            SparkSession spark = SparkSession.builder()
                .appName("Log Dataset Pipeline")
                .config("spark.sql.shuffle.partitions", 1)
                .master("local[*]")
                .getOrCreate();

            spark.listenerManager().register(new DbndSparkQueryExecutionListener());

            SQLContext sql = spark.sqlContext();

            String path = args.length > 0 ? args[0] : LogDatasetPipeline.class.getClassLoader().getResource("usa-education-budget.csv").getFile();
            Dataset<?> data = sql.read().option("inferSchema", "true").option("header", "true").csv(path);

            DbndLogger.logDatasetOperation("s3://databand/samples/usa-education-budget.csv", READ, OK, data, new LogDatasetRequest().withPreview().withSchema());
            DbndLogger.logDatasetOperation("file:///broken/path", WRITE, NOK, data, new RuntimeException());
            DbndLogger.logDatasetOperation("s3://datastore/sample1.json", READ, OK, data, new LogDatasetRequest().withPreview().withSchema().withPartition());
            DbndLogger.logDatasetOperation("s3://datastore/sample2.json", READ, OK, data, new LogDatasetRequest().withPreview().withSchema().withPartition(false));

            spark.stop();
        }

    }

    @Test
    public void testDatasets() throws IOException {
        PipelinesVerify pipelinesVerify = new PipelinesVerify();
        LogDatasetPipeline.main(new String[]{});

        String jobName = "log_dataset_pipeline";
        Job job = pipelinesVerify.verifyJob(jobName);
        Pair<Tasks, TaskFullGraph> tasksAndGraph = pipelinesVerify.verifyTasks(jobName, job);
        Tasks tasks = tasksAndGraph.left();

        pipelinesVerify.assertTaskExists(jobName, tasks, "success");
        Map<String, List<DatasetOperationRes>> datasetOpsByTask = pipelinesVerify.fetchDatasetOperations(job);

        pipelinesVerify.assertDatasetOperationExists(
            "log_dataset_pipeline",
            "file:///broken/path",
            DatasetOperationType.WRITE,
            "FAILED",
            1,
            1,
            datasetOpsByTask,
            "java.lang.RuntimeException",
            LogDataset.OP_SOURCE_JAVA_MANUAL_LOGGING
        );

        pipelinesVerify.assertDatasetOperationExists(
            "log_dataset_pipeline",
            "s3://datastore/sample2.json",
            DatasetOperationType.READ,
            "SUCCESS",
            1,
            1,
            datasetOpsByTask,
            LogDataset.OP_SOURCE_JAVA_MANUAL_LOGGING
        );

        pipelinesVerify.assertDatasetOperationExists(
            "log_dataset_pipeline",
            "s3://datastore/",
            DatasetOperationType.READ,
            "SUCCESS",
            1,
            1,
            datasetOpsByTask,
            LogDataset.OP_SOURCE_JAVA_MANUAL_LOGGING
        );

        DatasetOperationRes op = pipelinesVerify.assertDatasetOperationExists(
            "log_dataset_pipeline",
            "s3://databand/samples/usa-education-budget.csv",
            READ,
            "SUCCESS",
            41,
            1,
            datasetOpsByTask,
            LogDataset.OP_SOURCE_JAVA_MANUAL_LOGGING
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
