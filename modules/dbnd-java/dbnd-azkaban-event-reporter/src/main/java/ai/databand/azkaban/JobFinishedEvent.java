package ai.databand.azkaban;

import ai.databand.azkaban.events.JobFinished;
import ai.databand.config.DbndConfig;
import ai.databand.schema.AzkabanTaskContext;
import azkaban.execapp.FlowRunnerManager;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.Status;
import azkaban.flow.CommonJobProperties;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JobFinishedEvent implements AzkabanEvent {

    private String flowName;
    private String projectName;
    private String executionId;
    private String startTime;
    private String jobStatus;
    private String failureMessage;

    private String jobId;

    @Override
    public void track() {
        FlowRunnerManager flowRunnerManager = SERVICE_PROVIDER.getInstance(FlowRunnerManager.class);
        ExecutableFlow executableFlow = flowRunnerManager.getExecutableFlow(Integer.parseInt(executionId));
        String flowUuid = executableFlow.getInputProps().get(CommonJobProperties.FLOW_UUID);

        DbndConfig config = new DbndConfig();
        AzkabanTaskContext ctx = new AzkabanTaskContext(projectName, flowName, flowUuid, executionId, jobId, config);

        EventReporterAzkabanJob job = new EventReporterAzkabanJob(
            config, jobId, executionId, Status.FAILED.toString().equalsIgnoreCase(jobStatus), startTime
        );

        AzkabanEvent origin = new JobFinished(config, ctx, job, failureMessage);
        origin.track();
    }

    // Jackson Setters
    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public void setJobStatus(String jobStatus) {
        this.jobStatus = jobStatus;
    }

    public void setFailureMessage(String failureMessage) {
        this.failureMessage = failureMessage;
    }
}
