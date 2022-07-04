package ai.databand.examples;

import ai.databand.annotations.Task;
import ai.databand.log.DbndLogger;
import org.junit.jupiter.api.Test;

/**
 * The purpose of this test is to make sure static methods are instrumented correctly.
 */
public class StaticMethodsTest {

    private static class StaticMainPipeline {

        @Task("static_pipeline")
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
     * Don't need any checks here, just making sure no exceptions are thrown.
     */
    @Test
    public void testPipelineWithMain() {
        StaticMainPipeline.main(new String[]{});
    }

}
