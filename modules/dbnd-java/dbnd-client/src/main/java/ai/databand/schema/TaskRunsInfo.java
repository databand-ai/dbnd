package ai.databand.schema;

import java.util.List;

public class TaskRunsInfo {

    private final String taskRunEnvUid;

    private final List<List<String>> parentChildMap;

    private final String runUid;

    private final List<TaskRun> taskRuns;

    private final List<Target> targets;

    private final String rootRunUid;

    private final List<List<String>> upstreamsMap;

    private final boolean dynamicTaskRunUpdate;

    private final List<TaskDefinition> taskDefinitions;

    private final TrackingSource sourceContext;

    public TaskRunsInfo(String taskRunEnvUid,
                        List<List<String>> parentChildMap,
                        String runUid,
                        List<TaskRun> taskRuns,
                        List<Target> targets,
                        String rootRunUid,
                        List<List<String>> upstreamsMap,
                        boolean dynamicTaskRunUpdate,
                        List<TaskDefinition> taskDefinitions,
                        TrackingSource sourceContext) {
        this.taskRunEnvUid = taskRunEnvUid;
        this.parentChildMap = parentChildMap;
        this.runUid = runUid;
        this.taskRuns = taskRuns;
        this.targets = targets;
        this.rootRunUid = rootRunUid;
        this.upstreamsMap = upstreamsMap;
        this.dynamicTaskRunUpdate = dynamicTaskRunUpdate;
        this.taskDefinitions = taskDefinitions;
        this.sourceContext = sourceContext;
    }

    public String getTaskRunEnvUid() {
        return taskRunEnvUid;
    }

    public List<List<String>> getParentChildMap() {
        return parentChildMap;
    }

    public String getRunUid() {
        return runUid;
    }

    public List<TaskRun> getTaskRuns() {
        return taskRuns;
    }

    public List<Target> getTargets() {
        return targets;
    }

    public String getRootRunUid() {
        return rootRunUid;
    }

    public List<List<String>> getUpstreamsMap() {
        return upstreamsMap;
    }

    public boolean isDynamicTaskRunUpdate() {
        return dynamicTaskRunUpdate;
    }

    public List<TaskDefinition> getTaskDefinitions() {
        return taskDefinitions;
    }

    public TrackingSource getSourceContext() {
        return sourceContext;
    }
}
