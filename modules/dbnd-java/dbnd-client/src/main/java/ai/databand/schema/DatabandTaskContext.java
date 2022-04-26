package ai.databand.schema;

public class DatabandTaskContext {

    private final String rootRunUid;
    private final String taskRunUid;
    private final String taskRunAttemptUid;
    private final String traceId;

    public DatabandTaskContext(String rootRunUid, String taskRunUid, String taskRunAttemptUid, String traceId) {
        this.rootRunUid = rootRunUid;
        this.taskRunUid = taskRunUid;
        this.taskRunAttemptUid = taskRunAttemptUid;
        this.traceId = traceId;
    }

    public String getRootRunUid() {
        return rootRunUid;
    }

    public String getTaskRunUid() {
        return taskRunUid;
    }

    public String getTaskRunAttemptUid() {
        return taskRunAttemptUid;
    }

    public String getTraceId() {
        return traceId;
    }
}
