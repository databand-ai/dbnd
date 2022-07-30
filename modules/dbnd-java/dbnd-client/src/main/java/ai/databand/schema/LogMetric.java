/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

public class LogMetric {

    private final String taskRunAttemptUid;
    private final Metric metric;
    private final String source;

    public LogMetric(String taskRunAttemptUid, Metric metric, String source) {
        this.taskRunAttemptUid = taskRunAttemptUid;
        this.metric = metric;
        this.source = source;
    }

    public LogMetric(String taskRunAttemptUid, Metric metric) {
        this.taskRunAttemptUid = taskRunAttemptUid;
        this.metric = metric;
        this.source = null;
    }

    public Metric getMetric() {
        return metric;
    }

    public String getTaskRunAttemptUid() {
        return taskRunAttemptUid;
    }

    public String getSource() {
        return source;
    }
}
