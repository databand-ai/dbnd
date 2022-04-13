package ai.databand.examples;

import ai.databand.annotations.Task;
import ai.databand.log.DbndLogger;

public class StaticMainPipeline {

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
