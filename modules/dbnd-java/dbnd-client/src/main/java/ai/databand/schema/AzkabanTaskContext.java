package ai.databand.schema;

import ai.databand.config.DbndConfig;
import ai.databand.id.Uuid5;

import java.util.Optional;

public class AzkabanTaskContext {

    private final String projectName;
    private final String flowId;
    private final String flowUuid;
    private final String executionId;
    private final String jobId;
    private final DbndConfig config;

    public AzkabanTaskContext(String projectName,
                              String flowId,
                              String flowUuid,
                              String executionId,
                              String jobId,
                              DbndConfig config) {
        this.projectName = projectName;
        this.flowId = flowId;
        this.flowUuid = flowUuid;
        this.executionId = executionId;
        this.jobId = jobId;
        this.config = config;
    }

    public AzkabanTaskContext forJob(String jobId) {
        return new AzkabanTaskContext(
            this.projectName,
            this.flowId,
            this.flowUuid,
            this.executionId,
            jobId,
            config
        );
    }

    public TrackingSource trackingSource() {
        if (config.getValue("azkaban.name").isPresent()) {
            return new TrackingSource(this);
        } else {
            return null;
        }
    }

    public String databandJobName() {
        return String.format("%s.%s", flowId, jobId);
    }

    public String projectName() {
        return projectName;
    }

    public String flowId() {
        return flowId;
    }

    public String flowUuid() {
        return flowUuid;
    }

    public String executionId() {
        return executionId;
    }

    public String jobId() {
        return jobId;
    }

    public Optional<String> userId() {
        return Optional.of(System.getenv("azkaban.flow.submituser"));
    }

    public String azkabanInstanceId() {
        return String.format("%s:%s", config.getValue("azkaban.name").orElse("azkaban"), config.getValue("azkaban.label").orElse("azkaban"));
    }

    public String azkabanInstanceUuid() {
        return new Uuid5("AZ_INSTANCE_UUID", azkabanInstanceId()).toString();
    }

    public String azkabanUrl() {
        String hostName = config.getValue("server.hostname").orElse("localhost");
        String port = config.getValue("server.port").orElse("8081");
        String protocol = Boolean.TRUE.toString().equalsIgnoreCase(config.getValue("jetty.use.ssl").orElse(Boolean.FALSE.toString())) ? "https" : "http";
        return String.format("%s://%s:%s", protocol, hostName, port);
    }

    public RootRun root() {
        return new RootRun(
            "",
            taskRunUid(),
            rootRunUid(),
            taskRunAttemptUid()
        );
    }

    /**
     * project_name - flow_name - execution_id
     *
     * @return
     */
    public String runName() {
        return String.format("%s-%s-%s", projectName, flowId, executionId);
    }

    /**
     * project_name - flow_name - execution_id
     *
     * @return
     */
    public String jobRunName() {
        return String.format("%s-%s-%s-%s", projectName, flowId, jobId, executionId);
    }

    /**
     * Root run (flow) UID.
     *
     * @return
     */
    public String rootRunUid() {
        return new Uuid5("RUN_UID", flowUuid).toString();
    }

    /**
     * Driver task (flow) UID.
     *
     * @return
     */
    public String driverTaskUid() {
        return new Uuid5("DRIVER_TASK", flowUuid).toString();
    }

    public String driverTaskRunEnvUid() {
        return new Uuid5("TASK_RUN_ENV_UID", flowUuid).toString();
    }

    public String driverTaskDefinitionUid() {
        return new Uuid5("TASK_DEFINITION", flowUuid).toString();
    }

    /**
     * Driver task (flow) attempt UID.
     *
     * @return
     */
    public String driverTaskRunAttemptUid() {
        return new Uuid5("TASK_RUN_ATTEMPT", flowUuid).toString();
    }

    /**
     * Task (job) run UID.
     *
     * @return
     */
    public String taskRunUid() {
        String taskRunId = jobId + flowUuid;
        return new Uuid5("TASK_RUN_UID", taskRunId).toString();
    }

    /**
     * Task (job) run attempt UID.
     *
     * @return
     */
    public String taskRunAttemptUid() {
        String taskRunId = jobId + flowUuid;
        return new Uuid5("TASK_RUN_ATTEMPT", taskRunId).toString();
    }

    public String taskDefinitionUid() {
        String taskRunId = jobId + flowUuid;
        return new Uuid5("TASK_DEFINITION", taskRunId).toString();
    }
}
