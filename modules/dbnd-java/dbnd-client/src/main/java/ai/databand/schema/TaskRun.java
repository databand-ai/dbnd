package ai.databand.schema;

import ai.databand.schema.jackson.LocalDateDeserializer;
import ai.databand.schema.jackson.LocalDateSerializer;
import ai.databand.schema.jackson.ZonedDateTimeDeserializer;
import ai.databand.schema.jackson.ZonedDateTimeSerializer;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskRun {

    private String uid;

    private String runUid;

    private boolean isRoot;

    private boolean isSystem;

    private String logRemote;

    private String version;

    private String taskRunUid;

    private String taskSignature;

    private String name;

    private List<TaskRunParam> taskRunParams;

    private String outputSignature;

    private boolean isSkipped;

    @JsonSerialize(using = LocalDateSerializer.class)
    @JsonDeserialize(using = LocalDateDeserializer.class)
    private LocalDate targetDate;

    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private ZonedDateTime executionDate;

    private String logLocal;

    private String state;

    private String taskDefinitionUid;

    private String commandLine;

    private boolean isReused;

    private boolean hasUpstreams;

    private String taskRunAttemptUid;

    private String taskAfId;

    private boolean isDynamic;

    private boolean hasDownstreams;

    private String functionalCall;

    private String taskId;

    private String env;

    @JsonIgnore
    private StringBuilder logBuffer;

    @JsonIgnore
    private Map<String, Object> metrics;

    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private ZonedDateTime startDate;

    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private ZonedDateTime endDate;

    @JsonIgnore
    private List<TaskRun> upstreamTasks = new ArrayList<>(1);

    private Integer latestTaskRunAttemptId;

    private Map<String, String> externalLinks;

    public TaskRun() {
    }

    public TaskRun(String runUid,
                   boolean isRoot,
                   boolean isSystem,
                   String logRemote,
                   String version,
                   String taskRunUid,
                   String taskSignature,
                   String name,
                   List<TaskRunParam> taskRunParams,
                   String outputSignature,
                   boolean isSkipped,
                   LocalDate targetDate,
                   ZonedDateTime executionDate,
                   String logLocal,
                   String state,
                   String taskDefinitionUid,
                   String commandLine,
                   boolean isReused,
                   boolean hasUpstreams,
                   String taskRunAttemptUid,
                   String taskAfId,
                   boolean isDynamic,
                   boolean hasDownstreams,
                   String functionalCall,
                   String taskId,
                   String env,
                   Map<String, String> externalLinks) {
        this.runUid = runUid;
        this.isRoot = isRoot;
        this.isSystem = isSystem;
        this.logRemote = logRemote;
        this.version = version;
        this.taskRunUid = taskRunUid;
        this.taskSignature = taskSignature;
        this.name = name;
        this.taskRunParams = taskRunParams;
        this.outputSignature = outputSignature;
        this.isSkipped = isSkipped;
        this.targetDate = targetDate;
        this.executionDate = executionDate;
        this.logLocal = logLocal;
        this.state = state;
        this.taskDefinitionUid = taskDefinitionUid;
        this.commandLine = commandLine;
        this.isReused = isReused;
        this.hasUpstreams = hasUpstreams;
        this.taskRunAttemptUid = taskRunAttemptUid;
        this.taskAfId = taskAfId;
        this.isDynamic = isDynamic;
        this.hasDownstreams = hasDownstreams;
        this.functionalCall = functionalCall;
        this.taskId = taskId;
        this.env = env;
        this.externalLinks = externalLinks;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getRunUid() {
        return runUid;
    }

    public void setRunUid(String runUid) {
        this.runUid = runUid;
    }

    public boolean getIsRoot() {
        return isRoot;
    }

    public boolean getIsSystem() {
        return isSystem;
    }

    public String getLogRemote() {
        return logRemote;
    }

    public void setLogRemote(String logRemote) {
        this.logRemote = logRemote;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getTaskRunUid() {
        return taskRunUid;
    }

    public void setTaskRunUid(String taskRunUid) {
        this.taskRunUid = taskRunUid;
    }

    public String getTaskSignature() {
        return taskSignature;
    }

    public void setTaskSignature(String taskSignature) {
        this.taskSignature = taskSignature;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<TaskRunParam> getTaskRunParams() {
        return taskRunParams;
    }

    public void setTaskRunParams(List<TaskRunParam> taskRunParams) {
        this.taskRunParams = taskRunParams;
    }

    public String getOutputSignature() {
        return outputSignature;
    }

    public void setOutputSignature(String outputSignature) {
        this.outputSignature = outputSignature;
    }

    public boolean getIsSkipped() {
        return isSkipped;
    }

    public LocalDate getTargetDate() {
        return targetDate;
    }

    public void setTargetDate(LocalDate targetDate) {
        this.targetDate = targetDate;
    }

    public ZonedDateTime getExecutionDate() {
        return executionDate;
    }

    public void setExecutionDate(ZonedDateTime executionDate) {
        this.executionDate = executionDate;
    }

    public String getLogLocal() {
        return logLocal;
    }

    public void setLogLocal(String logLocal) {
        this.logLocal = logLocal;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getTaskDefinitionUid() {
        return taskDefinitionUid;
    }

    public void setTaskDefinitionUid(String taskDefinitionUid) {
        this.taskDefinitionUid = taskDefinitionUid;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public void setCommandLine(String commandLine) {
        this.commandLine = commandLine;
    }

    public boolean getIsReused() {
        return isReused;
    }

    public boolean getHasUpstreams() {
        return hasUpstreams;
    }

    public void setHasUpstreams(boolean hasUpstreams) {
        this.hasUpstreams = hasUpstreams;
    }

    public String getTaskRunAttemptUid() {
        return taskRunAttemptUid;
    }

    public void setTaskRunAttemptUid(String taskRunAttemptUid) {
        this.taskRunAttemptUid = taskRunAttemptUid;
    }

    public String getTaskAfId() {
        return taskAfId;
    }

    public void setTaskAfId(String taskAfId) {
        this.taskAfId = taskAfId;
    }

    public boolean getIsDynamic() {
        return isDynamic;
    }

    public boolean getHasDownstreams() {
        return hasDownstreams;
    }

    public void setHasDownstreams(boolean hasDownstreams) {
        this.hasDownstreams = hasDownstreams;
    }

    public String getFunctionalCall() {
        return functionalCall;
    }

    public void setFunctionalCall(String functionalCall) {
        this.functionalCall = functionalCall;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public void setRoot(boolean root) {
        isRoot = root;
    }

    public void setSystem(boolean system) {
        isSystem = system;
    }

    public void setSkipped(boolean skipped) {
        isSkipped = skipped;
    }

    public void setReused(boolean reused) {
        isReused = reused;
    }

    public void setDynamic(boolean dynamic) {
        isDynamic = dynamic;
    }

    public ZonedDateTime getStartDate() {
        return startDate;
    }

    public void setStartDate(ZonedDateTime startDate) {
        this.startDate = startDate;
    }

    public ZonedDateTime getEndDate() {
        return endDate;
    }

    public void setEndDate(ZonedDateTime endDate) {
        this.endDate = endDate;
    }

    public Integer getLatestTaskRunAttemptId() {
        return latestTaskRunAttemptId;
    }

    public void setLatestTaskRunAttemptId(Integer latestTaskRunAttemptId) {
        this.latestTaskRunAttemptId = latestTaskRunAttemptId;
    }

    public Map<String, String> getExternalLinks() {
        return externalLinks;
    }

    public void appendLog(String msg) {
        if (logBuffer == null) {
            logBuffer = new StringBuilder();
        }
        for (TaskRun upstream : upstreamTasks) {
            upstream.appendLog(msg);
        }
        logBuffer.append(msg);
    }

    @JsonIgnore
    public String getTaskLog() {
        if (logBuffer == null) {
            return null;
        }
        return logBuffer.toString();
    }

    public void appendPrefixedMetrics(Map<String, Object> values) {
        if (metrics == null) {
            metrics = new HashMap<>(1);
        }
        metrics.putAll(values);
    }

    public void appendMetrics(Map<String, Object> values) {
        for (TaskRun upstream : upstreamTasks) {
            upstream.appendMetrics(values);
        }
        if (metrics == null) {
            metrics = new HashMap<>(1);
            metrics.putAll(values);
            return;
        }
        for (Map.Entry<String, Object> m : values.entrySet()) {
            String key = m.getKey();
            if (metrics.containsKey(key)) {
                Object existingValue = metrics.get(key);
                if (existingValue instanceof Number) {
                    metrics.put(key, (Long) existingValue + (Long) m.getValue());
                }
            }
        }
    }

    @JsonIgnore
    public Map<String, Object> getMetrics() {
        if (metrics == null) {
            return Collections.emptyMap();
        }
        return metrics;
    }

    public void addUpstream(TaskRun upstream) {
        upstreamTasks.add(upstream);
    }
}
