/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
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

/**
 * This test checks log_dataset_op from listener when performed spark operation does not require schema.
 * For operations like `count` Spark Listener won't receive schema in the FileSourceScanExec node.
 * Schema should be extracted from the relation.
 */
public class ListenerSchemaTest {

    @BeforeAll
    static void setup() {
        if(!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            BasicConfigurator.configure();
        }
        Logger.getLogger("ai.databand").setLevel(Level.INFO);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.spark_project").setLevel(Level.WARN);
    }

    private static class TestSchemaPipeline {

        @Task("test_schema_pipeline")
        public static void main(String[] args) {
            SparkSession spark = SparkSession.builder()
                .appName("Log Dataset Pipeline")
                .config("spark.sql.shuffle.partitions", 1)
                .config("spark.env.DBND__LOG__PREVIEW_HEAD_BYTES", 32 * 1024)
                .config("spark.env.DBND__LOG__PREVIEW_TAIL_BYTES", 32 * 1024)
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

        /*
         FIXME Support for schema changes in Databand UI between logging dataset operations?

         There are two Spark file scans/operations - first one for getting column names and second one for reading actual data.
         Both are correctly detected by Databand agent and reported. Schema reported by the second data scan is different than the first data scan.
         Databand uses schema from the first data scan - the one for reading column names, instead of the one for data.

         ---

         First data scan:

            74751 [Test worker] INFO ai.databand.spark.DbndSparkQueryExecutionListener  - [[==DBND==]] v [1607747005] Processing event from function "head()" and execution plan class: org.apache.spark.sql.execution.CollectLimitExec. Executed plan: CollectLimit 1
                +- *(1) Filter (length(trim(value#8747, None)) > 0)
                    +- *(1) FileScan text [value#8747] Batched: false, Format: Text, Location: InMemoryFileIndex[file:/home/djania/git_repos/db1/databand-1/dbnd-core/modules/dbnd-java/dbnd-exa..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<value:string>

            74752 [Test worker] INFO ai.databand.spark.DbndSparkQueryExecutionListener  - [[==DBND==]] v spark metrics: FileScan text [value#8747] Batched: false, Format: Text, Location: InMemoryFileIndex[file:/home/djania/git_repos/db1/databand-1/dbnd-core/modules/dbnd-java/dbnd-exa..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<value:string>

            75013 [Test worker] INFO ai.databand.DbndWrapper  - [[==DBND==]] Dataset Operation [path: 'file:/home/djania/git_repos/db1/databand-1/dbnd-core/modules/dbnd-java/dbnd-examples/build/resources/test/usa-education-budget.csv'], [type: read], [status: OK] logged, schema: {"columns":["value"],"dtypes":{"value":"string"},"shape":[1,1],"type":"Spark.DataFrame"}, dimensions: [1, 1], stats: [ai.databand.schema.ColumnStats@173e347f]

        Second data scan:

            75120 [Test worker] INFO ai.databand.spark.DbndSparkQueryExecutionListener  - [[==DBND==]] v [-818460267] Processing event from function "count()" and execution plan class: org.apache.spark.sql.execution.WholeStageCodegenExec. Executed plan: *(2) HashAggregate(keys=[], functions=[count(1)], output=[count#8770L])
                +- Exchange SinglePartition
                    +- *(1) HashAggregate(keys=[], functions=[partial_count(1)], output=[count#8773L])
                        +- *(1) FileScan csv [] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/djania/git_repos/db1/databand-1/dbnd-core/modules/dbnd-java/dbnd-exa..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<>

            75121 [Test worker] INFO ai.databand.spark.DbndSparkQueryExecutionListener  - [[==DBND==]] v spark metrics: FileScan csv [] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/djania/git_repos/db1/databand-1/dbnd-core/modules/dbnd-java/dbnd-exa..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<>

            75354 [Test worker] INFO ai.databand.DbndWrapper  - [[==DBND==]] Dataset Operation [path: 'file:/home/djania/git_repos/db1/databand-1/dbnd-core/modules/dbnd-java/dbnd-examples/build/resources/test/usa-education-budget.csv'], [type: read], [status: OK] logged, schema: {"columns":["YEAR","BUDGET_ON_EDUCATION","GDP","RATIO"],"dtypes":{"RATIO":"double","GDP":"double","YEAR":"integer","BUDGET_ON_EDUCATION":"double"},"shape":[41,4],"type":"Spark.DataFrame"}, dimensions: [41, 4], stats: [ai.databand.schema.ColumnStats@56e71576, ai.databand.schema.ColumnStats@519f665e, ai.databand.schema.ColumnStats@17da4d27, ai.databand.schema.ColumnStats@42bf98f0]
         */

        pipelinesVerify.assertListenerColumnStat(op.getColumnsStats(), "RATIO", "double", 41);
        pipelinesVerify.assertListenerColumnStat(op.getColumnsStats(), "YEAR", "integer", 41);
        pipelinesVerify.assertListenerColumnStat(op.getColumnsStats(), "GDP", "double", 41);
        pipelinesVerify.assertListenerColumnStat(op.getColumnsStats(), "RATIO", "double", 41);
    }
}
