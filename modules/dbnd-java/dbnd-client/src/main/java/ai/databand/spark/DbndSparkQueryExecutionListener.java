package ai.databand.spark;

import ai.databand.DbndWrapper;
import ai.databand.parameters.DatasetOperationPreview;
import ai.databand.schema.ColumnStats;
import ai.databand.schema.DatasetOperationStatus;
import ai.databand.schema.DatasetOperationType;
import ai.databand.schema.LogDataset;
import ai.databand.schema.Pair;
import org.apache.spark.sql.catalyst.plans.QueryPlan;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import org.apache.spark.sql.execution.command.DataWritingCommandExec;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.hive.execution.HiveTableScanExec;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static ai.databand.DbndPropertyNames.DBND_INTERNAL_ALIAS;

public class DbndSparkQueryExecutionListener implements QueryExecutionListener {

    private static final Logger LOG = LoggerFactory.getLogger(DbndSparkQueryExecutionListener.class);

    private final DbndWrapper dbnd;
    private final DatasetOperationPreview operationPreview;
    private final boolean isHiveEnabled;

    public DbndSparkQueryExecutionListener(DbndWrapper dbnd) {
        this.dbnd = dbnd;
        this.operationPreview = new DatasetOperationPreview();
        // test if Hive is installed on cluster to avoid ClassNotFoundException in runtime
        try {
            Class.forName("org.apache.spark.sql.hive.execution.HiveTableScanExec", false, getClass().getClassLoader());
            Class.forName("org.apache.spark.sql.hive.execution.InsertIntoHiveTable", false, getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            isHiveEnabled = false;
            return;
        }
        isHiveEnabled = true;
    }

    public DbndSparkQueryExecutionListener() {
        this(DbndWrapper.instance());
    }

    @Override
    public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
        // Instanceof chain used instead of pattern-matching for the sake of simplicity here.
        // When new implementations will be added, this part should be refactored to use pattern-matching (strategies)
        SparkPlan executedPlan = qe.executedPlan();
        if (isAdaptivePlan(executedPlan)) {
            executedPlan = extractFinalFromAdaptive(executedPlan).orElse(executedPlan);
        }
        if (executedPlan instanceof DataWritingCommandExec) {
            DataWritingCommandExec writePlan = (DataWritingCommandExec) executedPlan;
            if (writePlan.cmd() instanceof InsertIntoHadoopFsRelationCommand) {
                InsertIntoHadoopFsRelationCommand cmd = (InsertIntoHadoopFsRelationCommand) writePlan.cmd();

                String path = exctractPath(cmd.outputPath().toString());
                long rows = cmd.metrics().get("numOutputRows").get().value();

                log(path, DatasetOperationType.WRITE, cmd.query().schema(), rows);
            }
            if (isHiveEnabled) {
                if (writePlan.cmd() instanceof InsertIntoHiveTable) {
                    try {
                        InsertIntoHiveTable cmd = (InsertIntoHiveTable) writePlan.cmd();

                        String path = cmd.table().identifier().table();
                        long rows = cmd.metrics().get("numOutputRows").get().value();

                        log(path, DatasetOperationType.WRITE, cmd.query().schema(), rows);
                    } catch (Exception e) {
                        LOG.error("Unable to extract dataset information from InsertIntoHiveTable", e);
                    }
                }
            }
        }
        if (executedPlan instanceof WholeStageCodegenExec) {
            if (isDbndPlan(qe)) {
                LOG.warn("dbnd sdk Execution plan will not be reported");
                return;
            }
            List<SparkPlan> allChildren = getAllChildren(executedPlan);
            for (SparkPlan next : allChildren) {
                if (next instanceof FileSourceScanExec) {
                    FileSourceScanExec fileSourceScan = (FileSourceScanExec) next;

                    String path = exctractPath(fileSourceScan.metadata().get("Location").get());
                    long rows = fileSourceScan.metrics().get("numOutputRows").get().value();

                    log(path, DatasetOperationType.READ, fileSourceScan.schema(), rows);
                }
                if (isHiveEnabled) {
                    if (next instanceof HiveTableScanExec) {
                        try {
                            HiveTableScanExec hiveTableScan = (HiveTableScanExec) next;

                            // Hive Table name, may be reused in future
                            // String tableName = hiveTableScan.relation().tableMeta().identifier().table();

                            // Path resolved by Hive Metastore
                            String path = hiveTableScan.relation().tableMeta().storage().locationUri().get().toString();
                            long rows = hiveTableScan.metrics().get("numOutputRows").get().value();

                            log(path, DatasetOperationType.READ, hiveTableScan.schema(), rows);
                        } catch (Exception e) {
                            LOG.error("Unable to extract dataset information from HiveTableScanExec", e);
                        }
                    }
                }
            }
        }
    }

    private boolean isDbndPlan(QueryExecution qe) {
        if (qe.analyzed() != null && !qe.analyzed().children().isEmpty()) {
            String dfAlias = getPlanVerboseString(qe.analyzed().children().apply(0));
            return dfAlias != null && dfAlias.contains(DBND_INTERNAL_ALIAS);
        }
        return false;
    }

    protected boolean isAdaptivePlan(Object plan) {
        return plan.getClass().getName().contains("AdaptiveSparkPlanExec");
    }

    /**
     * This is workaround for Spark 3.
     * verboseString() method was replaced in Spark 3 by verboseString(int).
     *
     * @param plan
     * @return
     */
    protected String getPlanVerboseString(LogicalPlan plan) {
        try {
            return plan.verboseString();
        } catch (NoSuchMethodError e) {
            // we're in spark 3
            try {
                Class<?> clazz = QueryPlan.class;
                Method method = clazz.getDeclaredMethod("verboseString", int.class);
                return method.invoke(plan, 1).toString();
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                LOG.error("Unable to identify whenever spark query was triggered by log_dataset_op or not", ex);
                return null;
            }
        }
    }

    /**
     * Adaptive plans was introduced in Spark 2.4.8+ and turned on by default on some configurations.
     * For proper reporting we use reflection to extract relevant pieces from Adaptive Plans.
     * Reflection allows us to avoid direct dependency bump.
     *
     * @param adaptivePlan
     * @return
     */
    protected Optional<SparkPlan> extractFinalFromAdaptive(SparkPlan adaptivePlan) {
        try {
            Class<?> clazz = Class.forName("org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec");
            Field field = clazz.getDeclaredField("currentPhysicalPlan");
            field.setAccessible(true);
            SparkPlan value = (SparkPlan) field.get(adaptivePlan);
            return Optional.of(value);
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            LOG.error("Unable to extract final plan from the adaptive one using reflection. Dataset operation won't be logged.", e);
            return Optional.empty();
        }
    }

    protected void log(String path, DatasetOperationType operationType, StructType datasetSchema, long rows) {
        Pair<String, List<Long>> schema = operationPreview.extractSchema(datasetSchema, rows);
        List<ColumnStats> columnStats = operationPreview.extractColumnStats(datasetSchema, rows);
        dbnd.logDatasetOperation(
            path,
            operationType,
            DatasetOperationStatus.OK,
            "",
            schema.right(),
            schema.left(),
            true,
            columnStats,
            LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
        );
    }

    /**
     * Extract data path from spark query plan.
     *
     * @param path
     * @return
     */
    protected String exctractPath(String path) {
        if (path.contains("InMemoryFileIndex")) {
            path = path.replace("InMemoryFileIndex[", "");
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    /**
     * DFS across spark plan nodes.
     *
     * @param root
     * @return
     */
    protected List<SparkPlan> getAllChildren(SparkPlan root) {
        List<SparkPlan> result = new ArrayList<>();
        Deque<SparkPlan> deque = new LinkedList<>();
        deque.add(root);
        while (!deque.isEmpty()) {
            SparkPlan next = deque.pop();
            result.add(next);
            if (next.getClass().getName().contains("ShuffleQueryStageExec")) {
                // Spark 3 feature
                Optional<SparkPlan> shuffleChild = extractChildFromShuffle(next);
                shuffleChild.ifPresent(deque::add);
            } else {
                List<SparkPlan> children = scala.collection.JavaConverters.seqAsJavaListConverter(next.children()).asJava();
                deque.addAll(children);
            }
        }
        return result;
    }

    /**
     * Shuffle queries was introduced in Spark 3 and doesn't have "children" nodes.
     * Instead, the child node can be accessed using plan() method.
     * We use reflection to gain access to this method in runtime and avoid direct dependency to Spark 3.
     *
     * @param shuffleQuery
     * @return
     */
    protected Optional<SparkPlan> extractChildFromShuffle(SparkPlan shuffleQuery) {
        try {
            Class<?> clazz = Class.forName("org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec");
            Method method = clazz.getDeclaredMethod("plan");
            SparkPlan value = (SparkPlan) method.invoke(shuffleQuery);
            return Optional.of(value);
        } catch (ClassNotFoundException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException e) {
            LOG.error("Unable to extract child plan from the shuffle query using reflection. Dataset operation won't be logged.", e);
            return Optional.empty();
        }
    }

    @Override
    public void onFailure(String funcName, QueryExecution qe, Exception exception) {
        // not implemented yet
    }
}
