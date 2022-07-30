/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

public class InitRunArgs {

    private final String runUid;
    private final String rootRunUid;

    private final String driverTaskUid;

    private final TaskRunEnv taskRunEnv;
    private final TaskRunsInfo taskRunsInfo;

    private final NewRunInfo newRunInfo;

    private final AirflowTaskContext afContext;
    private final String source;
    private final TrackingSource trackingSource;

    public InitRunArgs(String runUid,
                       String rootRunUid,
                       String driverTaskUid,
                       NewRunInfo newRunInfo,
                       TaskRunEnv taskRunEnv,
                       TaskRunsInfo taskRunsInfo,
                       AirflowTaskContext afContext,
                       String source,
                       TrackingSource trackingSource) {
        this.runUid = runUid;
        this.rootRunUid = rootRunUid;
        this.driverTaskUid = driverTaskUid;
        this.newRunInfo = newRunInfo;
        this.taskRunEnv = taskRunEnv;
        this.taskRunsInfo = taskRunsInfo;
        this.afContext = afContext;
        this.source = source;
        this.trackingSource = trackingSource;
    }

    public InitRunArgs(String runUid,
                       String rootRunUid,
                       String driverTaskUid,
                       NewRunInfo newRunInfo,
                       TaskRunEnv taskRunEnv,
                       TaskRunsInfo taskRunsInfo) {
        this(runUid,
            rootRunUid,
            driverTaskUid,
            newRunInfo,
            taskRunEnv,
            taskRunsInfo,
            null,
            null,
            null
        );
    }

    public String getRunUid() {
        return runUid;
    }

    public String getRootRunUid() {
        return rootRunUid;
    }

    public String getDriverTaskUid() {
        return driverTaskUid;
    }

    public NewRunInfo getNewRunInfo() {
        return newRunInfo;
    }

    public TaskRunEnv getTaskRunEnv() {
        return taskRunEnv;
    }

    public TaskRunsInfo getTaskRunsInfo() {
        return taskRunsInfo;
    }

    public AirflowTaskContext getAfContext() {
        return afContext;
    }

    public TrackingSource getTrackingSource() {
        return trackingSource;
    }

    public String getSource() {
        return source;
    }

}
