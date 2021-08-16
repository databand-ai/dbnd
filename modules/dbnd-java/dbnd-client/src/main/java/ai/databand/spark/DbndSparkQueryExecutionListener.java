package ai.databand.spark;

import ai.databand.DbndWrapper;
import ai.databand.parameters.DatasetOperationPreview;
import ai.databand.schema.DatasetOperationStatus;
import ai.databand.schema.DatasetOperationType;
import ai.databand.schema.Pair;
import org.apache.spark.sql.execution.CollectLimitExec;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import org.apache.spark.sql.execution.command.DataWritingCommandExec;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.util.QueryExecutionListener;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class DbndSparkQueryExecutionListener implements QueryExecutionListener {

    private final DbndWrapper dbnd;
    private final DatasetOperationPreview operationPreview;

    public DbndSparkQueryExecutionListener(DbndWrapper dbnd) {
        this.dbnd = dbnd;
        this.operationPreview = new DatasetOperationPreview();
    }

    public DbndSparkQueryExecutionListener() {
        this(DbndWrapper.instance());
    }

    @Override
    public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
        if (qe.executedPlan() instanceof DataWritingCommandExec) {
            DataWritingCommandExec writePlan = (DataWritingCommandExec) qe.executedPlan();
            if (writePlan.cmd() instanceof InsertIntoHadoopFsRelationCommand) {
                InsertIntoHadoopFsRelationCommand cmd = (InsertIntoHadoopFsRelationCommand) writePlan.cmd();

                String path = exctractPath(cmd.outputPath().toString());

                long rows = cmd.metrics().get("numOutputRows").get().value();

                Pair<String, List<Long>> schema = operationPreview.extractSchema(cmd.query().schema(), rows);

                dbnd.logDatasetOperation(
                    path,
                    DatasetOperationType.WRITE,
                    DatasetOperationStatus.OK,
                    "",
                    schema.right(),
                    schema.left()
                );
            }
        }
        if (qe.executedPlan() instanceof WholeStageCodegenExec || qe.executedPlan() instanceof CollectLimitExec) {
            List<SparkPlan> allChildren = getAllChildren(qe.executedPlan());
            for (SparkPlan next : allChildren) {
                if (next instanceof FileSourceScanExec) {
                    FileSourceScanExec fileSourceScan = (FileSourceScanExec) next;

                    String path = exctractPath(fileSourceScan.metadata().get("Location").get());

                    long rows = fileSourceScan.metrics().get("numOutputRows").get().value();
                    Pair<String, List<Long>> schema = operationPreview.extractSchema(fileSourceScan.schema(), rows);

                    dbnd.logDatasetOperation(
                        path,
                        DatasetOperationType.READ,
                        DatasetOperationStatus.OK,
                        "",
                        schema.right(),
                        schema.left()
                    );
                }
            }
        }
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
            List<SparkPlan> children = scala.collection.JavaConverters.seqAsJavaListConverter(next.children()).asJava();
            deque.addAll(children);
        }
        return result;
    }

    @Override
    public void onFailure(String funcName, QueryExecution qe, Exception exception) {
        // not implemented yet
    }
}
