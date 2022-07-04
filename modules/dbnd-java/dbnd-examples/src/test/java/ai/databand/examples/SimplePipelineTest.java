package ai.databand.examples;

import ai.databand.schema.Job;
import ai.databand.schema.Pair;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.TaskRun;
import ai.databand.schema.Tasks;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.SecureRandom;

/**
 * Test upstream-downstream relations.
 */
class SimplePipelineTest {

    @Test
    public void testPipeline() throws IOException {
        SecureRandom random = new SecureRandom();
        int firstValue = random.nextInt(100000);
        String secondValue = String.valueOf(random.nextInt(100000));

        SimplePipeline pipeline = new SimplePipeline(firstValue, secondValue);
        pipeline.doStuff();

        PipelinesVerify pipelinesVerify = new PipelinesVerify();

        String jobName = "simple_pipeline";

        Job job = pipelinesVerify.verifyJob("simple_pipeline");
        Pair<Tasks, TaskFullGraph> tasks = pipelinesVerify.verifyTasks(jobName, job);

        Assertions.assertNotNull(tasks, "Tasks response from dbnd should not be empty.");

        TaskRun stepOne = pipelinesVerify.assertTaskExists("stepOne", tasks.left(), "success");

        pipelinesVerify.assertParamInTask(stepOne, "arg0", String.valueOf(firstValue));
        pipelinesVerify.assertParamInTask(stepOne, "arg1", secondValue);

        TaskRun stepTwo = pipelinesVerify.assertTaskExists("stepTwo", tasks.left(), "success");
        pipelinesVerify.assertTaskExists("firstNestedTask", tasks.left(), "success");
        pipelinesVerify.assertTaskExists("thirdNestedTask", tasks.left(), "success");

        pipelinesVerify.assertUpstreams(stepOne, stepTwo, tasks.right());
    }

}
