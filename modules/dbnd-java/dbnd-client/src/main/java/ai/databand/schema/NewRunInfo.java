/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import ai.databand.schema.jackson.ZonedDateTimeDeserializer;
import ai.databand.schema.jackson.ZonedDateTimeSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.ZonedDateTime;

public class NewRunInfo {

    private String runUid;

    private String jobName;

    private String user;

    private String name;

    private String description;

    private String state;

    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private ZonedDateTime startTime;

    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private ZonedDateTime endTime;

    /**
     * To deprecate.
     */
    private String dagId;

    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private ZonedDateTime executionDate;

    /**
     * Task attributes.
     */
    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private ZonedDateTime targetDate;

    private String version;

    private String driverName;

    private boolean isArchived;

    private String envName;

    private String cloudType;

    private String trigger;

    private RootRun rootRun;

    private String scheduledRun;

    private boolean sendsHeartbeat;

    private String taskExecutor;

    private String projectName;

    public NewRunInfo(String scheduledRun,
                      String envName,
                      ZonedDateTime endTime,
                      String taskExecutor,
                      ZonedDateTime targetDate,
                      ZonedDateTime executionDate,
                      boolean sendsHeartbeat,
                      String driverName,
                      boolean isArchived,
                      String runUid,
                      String cloudType,
                      String trigger,
                      String version,
                      String jobName,
                      String user,
                      String description,
                      String name,
                      String state,
                      ZonedDateTime startTime,
                      RootRun rootRun) {
        this(
            scheduledRun,
            envName,
            endTime,
            taskExecutor,
            targetDate,
            executionDate,
            sendsHeartbeat,
            driverName,
            isArchived,
            runUid,
            cloudType,
            trigger,
            version,
            jobName,
            user,
            description,
            name,
            state,
            startTime,
            rootRun,
            null);
    }

    public NewRunInfo(String scheduledRun,
                      String envName,
                      ZonedDateTime endTime,
                      String taskExecutor,
                      ZonedDateTime targetDate,
                      ZonedDateTime executionDate,
                      boolean sendsHeartbeat,
                      String driverName,
                      boolean isArchived,
                      String runUid,
                      String cloudType,
                      String trigger,
                      String version,
                      String jobName,
                      String user,
                      String description,
                      String name,
                      String state,
                      ZonedDateTime startTime,
                      RootRun rootRun,
                      String projectName) {
        this.scheduledRun = scheduledRun;
        this.envName = envName;
        this.endTime = endTime;
        this.taskExecutor = taskExecutor;
        this.targetDate = targetDate;
        this.executionDate = executionDate;
        this.sendsHeartbeat = sendsHeartbeat;
        this.driverName = driverName;
        this.isArchived = isArchived;
        this.runUid = runUid;
        this.cloudType = cloudType;
        this.trigger = trigger;
        this.version = version;
        this.jobName = jobName;
        this.user = user;
        this.description = description;
        this.name = name;
        this.state = state;
        this.startTime = startTime;
        this.rootRun = rootRun;
        this.projectName = projectName;
    }

    public String getScheduledRun() {
        return scheduledRun;
    }

    public String getEnvName() {
        return envName;
    }

    public ZonedDateTime getEndTime() {
        return endTime;
    }

    public String getTaskExecutor() {
        return taskExecutor;
    }

    public ZonedDateTime getTargetDate() {
        return targetDate;
    }

    public ZonedDateTime getExecutionDate() {
        return executionDate;
    }

    public boolean isSendsHeartbeat() {
        return sendsHeartbeat;
    }

    public String getDriverName() {
        return driverName;
    }

    public boolean isArchived() {
        return isArchived;
    }

    public String getRunUid() {
        return runUid;
    }

    public String getCloudType() {
        return cloudType;
    }

    public String getTrigger() {
        return trigger;
    }

    public String getVersion() {
        return version;
    }

    public String getJobName() {
        return jobName;
    }

    public String getUser() {
        return user;
    }

    public String getDescription() {
        return description;
    }

    public String getDagId() {
        return "";
    }

    public String getName() {
        return name;
    }

    public String getState() {
        return state;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public RootRun getRootRun() {
        return rootRun;
    }

    public String getProjectName() {
        return projectName;
    }
}
