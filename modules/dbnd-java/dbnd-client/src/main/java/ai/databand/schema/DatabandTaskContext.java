package ai.databand.schema;

public class DatabandTaskContext {

    private final String rootRunUid;
    private final String taskRunUid;
    private final String taskRunAttemptUid;

    public DatabandTaskContext(String rootRunUid, String taskRunUid, String taskRunAttemptUid) {
        this.rootRunUid = rootRunUid;
        this.taskRunUid = taskRunUid;
        this.taskRunAttemptUid = taskRunAttemptUid;
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
}
