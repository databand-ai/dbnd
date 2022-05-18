package ai.databand.examples;

import ai.databand.annotations.Task;
import ai.databand.log.DbndLogger;
import org.junit.jupiter.api.Test;

public class StaticMethodsTest {

    private static class StaticMainPipeline {

        @Task
        public static void main(String args[]) {
            firstTask(null);
            secondTask();
        }

        @Task
        public static void firstTask(String arg) {
            DbndLogger.logMetric("key", "value");
        }

        @Task
        public static void secondTask() {

        }
    }

    /**
     * Ensure that pipeline with static methods only runs.
     */
    @Test
    public void testPipelineWithMain() {
        StaticMainPipeline.main(new String[]{});
    }

}
