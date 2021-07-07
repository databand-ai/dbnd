package ai.databand.spark;

import ai.databand.DbndWrapper;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.Tuple2;
import scala.collection.Iterator;

/**
 * Spark listener to report all spark-generated metrics for stages to Databand.
 * This class is safe to use in any circumstances and won't break client code.
 */
public class DbndSparkListener extends SparkListener {

    private final DbndWrapper dbnd;

    public DbndSparkListener(DbndWrapper dbnd) {
        this.dbnd = dbnd;
    }

    public DbndSparkListener() {
        this.dbnd = DbndWrapper.instance();
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        if (event instanceof SparkListenerSQLExecutionStart) {
            SparkListenerSQLExecutionStart sqlEvent = (SparkListenerSQLExecutionStart) event;
            // QueryExecution queryExecution = SQLExecution.getQueryExecution(sqlEvent.executionId());
            extractIoInfo(sqlEvent.sparkPlanInfo());
        }
    }

    /**
     * input location path
     * read schema
     * data format
     *
     * @param plan
     */
    protected void extractIoInfo(SparkPlanInfo plan) {
        Iterator<Tuple2<String, String>> iterator = plan.metadata().iterator();
        while (iterator.hasNext()) {
            Tuple2<String, String> next = iterator.next();
            if ("Location".equalsIgnoreCase(next._1())) {
                SparkIOSource source = new SparkIOSource(next._2(), "spark_plan_info");
                dbnd.logMetric(source.metricKey(), source);
            }
        }
        for (Iterator<SparkPlanInfo> it = plan.children().iterator(); it.hasNext(); ) {
            extractIoInfo(it.next());
        }
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        try {
            dbnd.logSpark(stageCompleted);
        } catch (Throwable e) {
            System.out.println("DbndSparkListener: Unable to log spark metrics");
            e.printStackTrace();
        }
    }

}
