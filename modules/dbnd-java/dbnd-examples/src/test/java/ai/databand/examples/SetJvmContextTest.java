/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.examples;

import ai.databand.schema.DatasetOperationRes;
import ai.databand.schema.DatasetOperationType;
import ai.databand.schema.Job;
import ai.databand.schema.LogDataset;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This test verifies that jvm context manager in python dbnd library properly sets context on task enter and exit.
 * Dataset operations captured here by the listener and each operation should be submitted to the proper task.
 */
public class SetJvmContextTest {

    private static final Logger LOG = LoggerFactory.getLogger(SetJvmContextTest.class);

    /**
     * This test takes python script and executes it via `spark-submit` command.
     * dbnd-spark python package should be installed for proper results.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testContext() throws IOException, InterruptedException {
        // this test won't run by default
        if (!"True".equals(System.getenv().get("DBND__ENABLE__SPARK_CONTEXT_ENV"))) {
            return;
        }

        String pyscript = getClass().getClassLoader().getResource("context_set_test.py").getPath();
        String data = getClass().getClassLoader().getResource("usa-education-budget.csv").getPath();

        String agentJar = System.getenv("AGENT_JAR");

        String cmd = "spark-submit " +
            "--conf spark.sql.queryExecutionListeners=ai.databand.spark.DbndSparkQueryExecutionListener " +
            "--conf spark.driver.extraJavaOptions=-javaagent:" + agentJar + " " +
            "--conf spark.sql.shuffle.partitions=1 " +
            pyscript + " " + data;
        LOG.info("Spark CMD: {}", cmd);
        Process exec = Runtime.getRuntime().exec(cmd);

        int returnCode = exec.waitFor();
        MatcherAssert.assertThat("Spark process should complete properly", returnCode, Matchers.equalTo(0));

        PipelinesVerify pipelinesVerify = new PipelinesVerify();
        String jobName = "context_set_test.py";
        Job job = pipelinesVerify.verifyJob(jobName);
        Map<String, List<DatasetOperationRes>> ops = pipelinesVerify.fetchDatasetOperations(job);

        pipelinesVerify.assertDatasetOperationExists(
            "parent_task",
            data.replace("/usa-education-budget.csv", ""),
            DatasetOperationType.READ,
            "SUCCESS",
            41,
            1,
            ops,
            null,
            LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
        );

        pipelinesVerify.assertDatasetOperationExists(
            "child_task",
            data.replace("/usa-education-budget.csv", ""),
            DatasetOperationType.READ,
            "SUCCESS",
            41,
            1,
            ops,
            null,
            LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
        );
    }
}
