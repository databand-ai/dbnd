package ai.databand.azkaban;

import ai.databand.azkaban.events.JobStarted;
import ai.databand.config.DbndConfig;
import ai.databand.schema.AzkabanTaskContext;
import azkaban.execapp.FlowRunnerManager;
import azkaban.executor.ExecutableFlow;
import azkaban.flow.CommonJobProperties;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JobStartedEvent implements AzkabanEvent {

    private String flowName;
    private String projectName;
    private String executionId;
    private String jobId;

    @Override
    public void track() {
        FlowRunnerManager flowRunnerManager = SERVICE_PROVIDER.getInstance(FlowRunnerManager.class);
        ExecutableFlow executableFlow = flowRunnerManager.getExecutableFlow(Integer.parseInt(executionId));
        String flowUuid = executableFlow.getInputProps().get(CommonJobProperties.FLOW_UUID);

        DbndConfig config = new DbndConfig();
        AzkabanTaskContext ctx = new AzkabanTaskContext(projectName, flowName, flowUuid, executionId, jobId, config);

        AzkabanEvent origin = new JobStarted(config, ctx);
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
}
