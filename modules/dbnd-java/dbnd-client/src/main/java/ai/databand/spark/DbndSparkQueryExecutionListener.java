/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
 */

package ai.databand.spark;

import static ai.databand.DbndPropertyNames.DBND_INTERNAL_ALIAS;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.catalyst.plans.QueryPlan;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.CollectLimitExec;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import org.apache.spark.sql.execution.command.DataWritingCommandExec;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.hive.execution.HiveTableScanExec;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.slf4j.LoggerFactory;

import ai.databand.DbndAppLog;
import ai.databand.DbndWrapper;
import ai.databand.parameters.DatasetOperationPreview;
import ai.databand.schema.ColumnStats;
import ai.databand.schema.DatasetOperationStatus;
import ai.databand.schema.DatasetOperationType;
import ai.databand.schema.LogDataset;
import ai.databand.schema.Pair;
import scala.collection.Map;

public class DbndSparkQueryExecutionListener implements QueryExecutionListener {

    private static final DbndAppLog LOG = new DbndAppLog(LoggerFactory.getLogger(DbndSparkQueryExecutionListener.class));

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

        LOG.jvmInfo("Succesfully constructed Databand QueryExecutionListener instance. Selected Spark events with dataset operations will be submitted to the Databand service.");
    }

    public DbndSparkQueryExecutionListener() {
        this(DbndWrapper.instance());
    }

    @Override
    public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
        boolean isProcessed = false;
        SparkPlan rootPlan = qe.executedPlan();

        LOG.verbose("[{}] Processing event from function \"{}()\" and execution plan class: {}. Executed root plan: {}", rootPlan.hashCode(), funcName, rootPlan.getClass().getName(), rootPlan);

        // Instanceof chain used instead of pattern-matching for the sake of simplicity here.
        // When new implementations will be added, this part should be refactored to use pattern-matching (strategies)
        if (isAdaptivePlan(rootPlan)) {
            rootPlan = extractFinalFromAdaptive(rootPlan).orElse(rootPlan);

            LOG.verbose("[{}] Extracted final root plan from adaptive plan. Final executed root plan: {}", rootPlan.hashCode(), rootPlan);
        }
        if (rootPlan instanceof DataWritingCommandExec) {
            LOG.verbose("[{}] rootPlan is instanceof DataWritingCommandExec", rootPlan.hashCode());

            isProcessed = submitReadWriteOps(rootPlan);
        }
        if (rootPlan instanceof WholeStageCodegenExec) {
            LOG.verbose("[{}] rootPlan is instanceof WholeStageCodegenExec", rootPlan.hashCode());
            if (isDbndPlan(qe)) {
                LOG.warn("[{}] Explicit Databand SDK DataFrame tracking will not be reported by JVM", rootPlan.hashCode());
                return;
            }
            isProcessed = submitReadWriteOps(rootPlan);
        }
        if (rootPlan.getClass().getName().equals("org.apache.spark.sql.execution.adaptive.ResultQueryStageExec")) {
            // Databricks Spark
            LOG.verbose("[{}] rootPlan is instanceof ResultQueryStageExec", rootPlan.hashCode());
            isProcessed = submitReadWriteOps(rootPlan);
        }
        /* It might need to be enabled if used by some customers  (SQLs with "limit X" statement)
        if (rootPlan instanceof CollectLimitExec) {
            LOG.verbose("[{}] rootPlan is instanceof CollectLimitExec", rootPlan.hashCode());
            isProcessed = submitReadWriteOps(rootPlan);
        }*/
        if (!isProcessed) {
            LOG.verbose("[{}] Spark event was not processed because root execution plan class {} and all its child plans are not supported", rootPlan.hashCode(), rootPlan.getClass().getName());
        } else {
            LOG.verbose("[{}] Spark event was processed succesfully, root execution plan class: {}", rootPlan.hashCode(), rootPlan.getClass().getName());
        }
    }

    protected boolean submitReadWriteOps(SparkPlan rootPlan) {
        boolean isProcessed = false;
        List<SparkPlan> allChildren = getAllChildren(rootPlan);
        LOG.verbose("[{}] {} children plans detected.", rootPlan.hashCode(), allChildren.size());

        for (SparkPlan next : allChildren) {
            if (next instanceof FileSourceScanExec) {
                LOG.verbose("[{}][{}] Supported FileSourceScanExec child plan type detected", rootPlan.hashCode(), next.hashCode());
                FileSourceScanExec fileSourceScan = (FileSourceScanExec) next;

                StructType schema = fileSourceScan.schema();
                // when performing operations like "count" schema is not reported in a FileSourceScanExec itself,
                // but we can try to figure it out from the HadoopRelation.
                if (schema.isEmpty() && fileSourceScan.relation() != null) {
                    schema = fileSourceScan.relation().schema();
                }

                String path = extractPath(fileSourceScan.metadata().get("Location").get());
                long rows = fileSourceScan.metrics().get("numOutputRows").get().value();

                log(path, DatasetOperationType.READ, schema, rows,false);
                isProcessed = true;
                continue;
            }
            if (next.getClass().getName().equals("com.databricks.photon.PhotonJsonFileScanExec")) {
                // Databricks Spark Photon engine
                LOG.verbose("[{}][{}] Supported PhotonJsonFileScanExec child plan type detected", rootPlan.hashCode(), next.hashCode());
                try {
                    Class[] argTypes = new Class[] {};
                    Method mschema = next.getClass().getMethod("schema", argTypes);
                    StructType schema = (StructType) mschema.invoke(next);
                    // when performing operations like "count" schema is not reported in a PhotonJsonFileScanExec itself,
                    // but we can try to figure it out from the HadoopRelation.
                    if (schema.isEmpty()) {
                        Method mrelation = next.getClass().getMethod("relation", argTypes);
                        HadoopFsRelation relation = (HadoopFsRelation) mrelation.invoke(next);
                        if (relation != null) {
                            schema = relation.schema();
                        }
                    }
                    Method mmetadata = next.getClass().getMethod("metadata", argTypes);
                    Map<String, String> meta = (Map<String, String>) mmetadata.invoke(next);

                    String path = extractPath(meta.get("Location").get());

                    Method mmetrics = next.getClass().getMethod("metrics", argTypes);
                    Map<String, SQLMetric> metrics = (Map<String, SQLMetric>) mmetrics.invoke(next);
                    long rows = metrics.get("numOutputRows").get().value();

                    log(path, DatasetOperationType.READ, schema, rows,false);
                    isProcessed = true;
                } catch (Exception e) {
                    LOG.error("[{}][{}] Unable to extract dataset information from PhotonJsonFileScanExec - {}", rootPlan.hashCode(), next.hashCode(), e);
                }
                continue;
            }
            if (isHiveEnabled) {
                if (next instanceof HiveTableScanExec) {
                    LOG.verbose("[{}][{}] Supported HiveTableScanExec child plan type detected", rootPlan.hashCode(), next.hashCode());
                    try {
                        HiveTableScanExec hiveTableScan = (HiveTableScanExec) next;

                        // Hive Table name, may be reused in future
                        // String tableName = hiveTableScan.relation().tableMeta().identifier().table();

                        // Path resolved by Hive Metastore
                        String path = hiveTableScan.relation().tableMeta().storage().locationUri().get().toString();
                        long rows = hiveTableScan.metrics().get("numOutputRows").get().value();

                        log(path, DatasetOperationType.READ, hiveTableScan.relation().schema(), rows,true);
                        isProcessed = true;
                    } catch (Exception e) {
                        LOG.error("[{}][{}] Unable to extract dataset information from HiveTableScanExec - {}", rootPlan.hashCode(), next.hashCode(), e);
                    }
                    continue;
                }
            }
            if (next instanceof DataWritingCommandExec) {
                LOG.verbose("[{}][{}] Supported DataWritingCommandExec child plan type detected", rootPlan.hashCode(), next.hashCode());

                DataWritingCommandExec writePlan = (DataWritingCommandExec) next;
                if (writePlan.cmd() instanceof InsertIntoHadoopFsRelationCommand) {
                    LOG.verbose("[{}][{}] writeplan is instanceof InsertIntoHadoopFsRelationCommand", rootPlan.hashCode(), writePlan.hashCode());
                    InsertIntoHadoopFsRelationCommand cmd = (InsertIntoHadoopFsRelationCommand) writePlan.cmd();

                    String path = extractPath(cmd.outputPath().toString());
                    long rows = cmd.metrics().get("numOutputRows").get().value();

                    log(path, DatasetOperationType.WRITE, cmd.query().schema(), rows,false);
                    isProcessed = true;
                }
                if (isHiveEnabled) {
                    if (writePlan.cmd() instanceof InsertIntoHiveTable) {
                        LOG.verbose("[{}][{}] writeplan is instanceof InsertIntoHiveTable", rootPlan.hashCode(), writePlan.hashCode());
                        try {
                            InsertIntoHiveTable cmd = (InsertIntoHiveTable) writePlan.cmd();

                            String path = cmd.table().identifier().table();
                            long rows = cmd.metrics().get("numOutputRows").get().value();

                            log(path, DatasetOperationType.WRITE, cmd.query().schema(), rows,true);
                            isProcessed = true;
                        } catch (Exception e) {
                            LOG.error("[{}][{}] Unable to extract dataset information from InsertIntoHiveTable - {}", rootPlan.hashCode(), writePlan.hashCode(), e);
                        }
                    }
                }
                continue;
            }

            LOG.verbose("[{}][{}] Unsupported children plan: {}", rootPlan.hashCode(), next.hashCode(), next.getClass().getName());
        }
        return isProcessed;
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
            LOG.error("[{}] Unable to extract final plan from the adaptive one using reflection. Dataset operation won't be logged - {}.", adaptivePlan.hashCode(), e);
            return Optional.empty();
        }
    }

    protected void log(String path, DatasetOperationType operationType, StructType datasetSchema, long rows,boolean isPartitioned) {
        Pair<String, List<Long>> schema = operationPreview.extractSchema(datasetSchema, rows);
        List<ColumnStats> columnStats = operationPreview.extractColumnStats(datasetSchema, rows);
        dbnd.logDatasetOperation(
            path,
            operationType,
            DatasetOperationStatus.OK,
            "",
            schema.right(),
            schema.left(),
            isPartitioned,
            columnStats,
            LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
        );
    }

    private final Pattern DATASET = Pattern.compile(".*\\[(.*)\\].*");

    /**
     * Extract data path from spark query plan.
     *
     * @param path
     * @return
     */
    protected String extractPath(String path) {
        Matcher matcher = DATASET.matcher(path);
        if (matcher.matches()) {
            return matcher.group(1);
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
        List<String> spark3classes = Arrays.asList("org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec", "org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec", "org.apache.spark.sql.execution.adaptive.ResultQueryStageExec");
        while (!deque.isEmpty()) {
            SparkPlan next = deque.pop();
            result.add(next);
            if (spark3classes.contains(next.getClass().getName())) {
                // Spark 3 feature
                Optional<SparkPlan> spark3Child = extractChildFromSpark3(next);
                spark3Child.ifPresent(deque::add);
                if(!spark3Child.isPresent()) {
                    LOG.verbose("[{}][{}] No child node for Spark3 node class '{}'", root.hashCode(), next.hashCode(), next.getClass().getName());
                }
            } else if (next.getClass().getName().contains("AdaptiveSparkPlanExec")) {
                Optional<SparkPlan> finalPlanFromAdaptive = extractFinalFromAdaptive(next);
                finalPlanFromAdaptive.ifPresent(deque::add);
            } else {
                List<SparkPlan> children = scala.collection.JavaConverters.seqAsJavaListConverter(next.children()).asJava();
                deque.addAll(children);
                if(children.size() == 0) {
                    // For some Spark nodes this message means new node type needs to be added to "spark3classes" list
                    LOG.verbose("[{}][{}] No children nodes for node '{}'", root.hashCode(), next.hashCode(), next.getClass().getName());
                }
            }
        }
        return result;
    }

    /**
     * Some queries were introduced in Spark 3 and doesn't have "children" nodes.
     * Instead, the child node can be accessed using plan() method.
     * We use reflection to gain access to this method in runtime and avoid direct dependency to Spark 3.
     *
     * @param spark3Query
     * @return
     */
    protected Optional<SparkPlan> extractChildFromSpark3(SparkPlan spark3Query) {
        try {
            Class<?> clazz = Class.forName(spark3Query.getClass().getName());
            Method method = clazz.getDeclaredMethod("plan");
            SparkPlan value = (SparkPlan) method.invoke(spark3Query);
            return Optional.of(value);
        } catch (ClassNotFoundException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException e) {
            LOG.error("[{}] Unable to extract child plan from the spark3 query '{}' using reflection. Dataset operation won't be logged - {}.", spark3Query.hashCode(), spark3Query.getClass().getName(), e);
            return Optional.empty();
        }
    }

    @Override
    public void onFailure(String funcName, QueryExecution qe, Exception exception) {
        // not implemented yet
    }
}
