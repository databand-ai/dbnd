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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This test verifies that jvm context manager in python dbnd library properly sets context on task enter and exit.
 * Dataset operations captured here by the listener and each operation should be submitted to the proper task.
 */
public class SetJvmContextTest {

    private static final Logger LOG = LoggerFactory.getLogger(SetJvmContextTest.class);

    private PipelinesVerify pipelinesVerify;
    private String pyscriptPath;
    private String dataPath;
    private String agentJar;
    private String examplesJar;

    @BeforeEach
    void setUp() throws IOException {
        pipelinesVerify = new PipelinesVerify();
        pyscriptPath = getClass().getClassLoader().getResource("context_set_test.py").getPath();
        dataPath = getClass().getClassLoader().getResource("usa-education-budget.csv").getPath();
        agentJar = System.getenv("AGENT_JAR");
        examplesJar = System.getenv("EXAMPLES_JAR");
    }

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
        String jobName = "test_set_jvm_context";
        ProcessBuilder pb = new ProcessBuilder()
            .command("spark-submit",
                "--conf", "spark.sql.queryExecutionListeners=ai.databand.spark.DbndSparkQueryExecutionListener",
                "--conf", "spark.driver.extraJavaOptions=-javaagent:" + agentJar,
                "--conf", "spark.sql.shuffle.partitions=1",
                pyscriptPath, dataPath, jobName
            );

        System.out.println("Spark CMD: " + String.join(" ", pb.command()));
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        Process exec = pb.start();

        int returnCode = exec.waitFor();
        MatcherAssert.assertThat("Spark process should complete properly", returnCode, Matchers.equalTo(0));

        Job job = pipelinesVerify.verifyJob(jobName);

        // ops are reporting to the corresponding tasks
        assertDatasetOps(job, "parent_task", dataPath.replace("/usa-education-budget.csv", ""), DatasetOperationType.READ, "SUCCESS", 41, 1);
        assertDatasetOps(job, "child_task", dataPath.replace("/usa-education-budget.csv", ""), DatasetOperationType.READ, "SUCCESS", 41, 1);
    }

    /**
     * This test ensures that dataset ops will be reported even if tracking will complete before listener will catch up.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testSlowListener() throws IOException, InterruptedException {
        // this test won't run by default
        if (!"True".equals(System.getenv().get("DBND__ENABLE__SPARK_CONTEXT_ENV"))) {
            return;
        }
        String jobName = "test_slow_listener";
        ProcessBuilder pb = new ProcessBuilder()
            .command("spark-submit",
                "--conf", "spark.sql.queryExecutionListeners=ai.databand.examples.SlowDbndSparkQueryExecutionListener",
                "--jars", agentJar + "," + examplesJar,
                "--conf", "spark.sql.shuffle.partitions=1",
                pyscriptPath, dataPath, jobName
            );

        System.out.println("Spark CMD: " + String.join(" ", pb.command()));
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        Process exec = pb.start();

        int returnCode = exec.waitFor();

        MatcherAssert.assertThat("Spark process should complete properly", returnCode, Matchers.equalTo(0));

        Job job = pipelinesVerify.verifyJob(jobName);

        // since context is returned to the very parent task, op will be reported to this task and will be merged with second op
        assertDatasetOps(job, jobName, dataPath.replace("/usa-education-budget.csv", ""), DatasetOperationType.READ, "SUCCESS", 41, 1);
    }

    protected void assertDatasetOps(Job job,
                                    String taskName,
                                    String dataPath,
                                    DatasetOperationType type,
                                    String status,
                                    int recordsCount,
                                    int operationsCount) throws IOException, InterruptedException {
        Map<String, List<DatasetOperationRes>> ops = null;
        int tries = 3;
        while (tries > 0) {
            ops = pipelinesVerify.fetchDatasetOperations(job);

            // wait until data ops calculation will be completed
            if (ops.getOrDefault(taskName, Collections.emptyList()).isEmpty()) {
                System.out.println("Waiting 5 seconds");
                TimeUnit.SECONDS.sleep(5);
                tries--;
                continue;
            }

            pipelinesVerify.assertDatasetOperationExists(
                taskName,
                dataPath,
                type,
                status,
                recordsCount,
                operationsCount,
                ops,
                null,
                LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
            );

            return;
        }
        Assertions.fail("Dataset ops was empty even after 15 seconds of timeout. Existing datasets: " + ops);
    }
}
