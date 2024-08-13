/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
 */

package ai.databand.spark;

import ai.databand.DbndAppLog;
import ai.databand.DbndWrapper;
import ai.databand.config.DbndConfig;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.collection.Iterator;

/**
 * Spark listener to report all spark-generated metrics for stages to Databand.
 * This class is safe to use in any circumstances and won't break client code.
 */
public class DbndSparkListener extends SparkListener {

    private static final DbndAppLog LOG = new DbndAppLog(LoggerFactory.getLogger(DbndSparkListener.class));

    private final DbndWrapper dbnd;

    public DbndSparkListener(DbndWrapper dbnd) {
        this.dbnd = dbnd;

        LOG.jvmInfo("Succesfully constructed Databand Listener instance. Selected Spark properties and metrics will be submitted to the Databand service.");
    }

    public DbndSparkListener() {
        this.dbnd = DbndWrapper.instance();

        LOG.jvmInfo("Succesfully constructed Databand Listener instance. Selected Spark properties and metrics will be submitted to the Databand service.");
    }

    /*@Override
    public void onOtherEvent(SparkListenerEvent event) {
        if (event instanceof SparkListenerSQLExecutionStart) {
            SparkListenerSQLExecutionStart sqlEvent = (SparkListenerSQLExecutionStart) event;
            // QueryExecution queryExecution = SQLExecution.getQueryExecution(sqlEvent.executionId());
            // SQL query info will be extracted by SQL Query Listener
            //extractIoInfo(sqlEvent.sparkPlanInfo());
        }
    }*/

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
    public void onJobStart(SparkListenerJobStart jobStart) {
        try {
            DbndConfig conf = dbnd.config();
            conf.setSparkProperties(jobStart.properties());
        } catch (Throwable e) {
            LOG.error("Failed to set Spark properties during onJobStart() handling:  {}", e);
        }
    }

    /*@Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        try {
            dbnd.logSpark(stageCompleted);
        } catch (Throwable e) {
            System.out.println("DbndSparkListener: Unable to log spark metrics");
            e.printStackTrace();
        }
    }*/

}
