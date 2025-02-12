/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
 */

package ai.databand.examples;

import ai.databand.annotations.Task;
import ai.databand.schema.Job;
import ai.databand.schema.Pair;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.TaskRun;
import ai.databand.schema.Tasks;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The purpose of this test is to make sure static methods are instrumented correctly.
 */
public class FailedPipelineTest {

    @BeforeAll
    static void setup() {
        if(!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            BasicConfigurator.configure();
        }
        Logger.getLogger("ai.databand").setLevel(Level.DEBUG);
    }

    private static class BadPipeline {

        @Task("java_bad_pipeline")
        public static void main(String args[]) {
            successTask();
            try {
                failureTask();
            } catch (Exception e) {
                // do nothing
            }
            throw new RuntimeException("Unable to complete the pipeline");
        }

        @Task
        public static void successTask() {
            // do nothing
        }

        @Task
        public static void failureTask() {
            throw new RuntimeException("Unable to complete the task");
        }
    }

    /**
     * Don't need any checks here, just making sure no exceptions are thrown.
     */
    @Test
    public void testFailedPipeline() throws IOException {
        Assertions.assertThrows(RuntimeException.class, () -> BadPipeline.main(new String[]{}));

        String jobName = "java_bad_pipeline";
        PipelinesVerify pipelinesVerify = new PipelinesVerify();
        Job job = pipelinesVerify.verifyJob(jobName);
        Pair<Tasks, TaskFullGraph> tasksAndGraph = pipelinesVerify.verifyTasks(jobName, job);
        Tasks tasks = tasksAndGraph.left();

        TaskRun executeBad = pipelinesVerify.assertTaskExists("java_bad_pipeline", tasks, "failed");

        Map<String, Integer> tasksAttemptsIds = tasks.getTaskInstances().values()
            .stream()
            .collect(Collectors.toMap(TaskRun::getUid, TaskRun::getLatestTaskRunAttemptId));

        pipelinesVerify.assertLogsInTask(
            tasksAttemptsIds.get(executeBad.getUid()),
            "java.lang.RuntimeException: Unable to complete the pipeline",
            true
        );

        pipelinesVerify.assertTaskExists("successTask", tasks, "success");
        TaskRun failureTask = pipelinesVerify.assertTaskExists("failureTask", tasks, "failed");

        pipelinesVerify.assertErrors(
            failureTask,
            tasks,
            "Unable to complete the task",
            "java.lang.RuntimeException: Unable to complete the task",
            "java.lang.RuntimeException"
        );

        pipelinesVerify.assertLogsInTask(
            tasksAttemptsIds.get(failureTask.getUid()),
            "java.lang.RuntimeException: Unable to complete the task",
            true
        );
    }

}
