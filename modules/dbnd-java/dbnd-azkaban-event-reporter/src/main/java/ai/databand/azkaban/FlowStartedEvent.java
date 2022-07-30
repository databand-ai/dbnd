/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban;

import ai.databand.azkaban.events.FlowStarted;
import ai.databand.azkaban.links.AzkabanLinks;
import ai.databand.config.DbndConfig;
import ai.databand.schema.AzkabanTaskContext;
import azkaban.execapp.FlowRunnerManager;
import azkaban.executor.ExecutableFlow;
import azkaban.flow.CommonJobProperties;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FlowStartedEvent implements AzkabanEvent {

    private String flowName;
    private String projectName;
    private String executionId;

    @Override
    public void track() {
        DbndConfig config = new DbndConfig();

        FlowRunnerManager flowRunnerManager = SERVICE_PROVIDER.getInstance(FlowRunnerManager.class);
        ExecutableFlow executableFlow = flowRunnerManager.getExecutableFlow(Integer.parseInt(executionId));
        String flowUuid = executableFlow.getInputProps().get(CommonJobProperties.FLOW_UUID);
        AzkabanLinks links = new EventReporterAzkabanLinks(executableFlow);
        AzkabanTaskContext ctx = new AzkabanTaskContext(projectName, flowName, flowUuid, executionId, "", config);

        AzkabanFlow flow = new EventReporterAzkabanFlow(config, links, ctx, executableFlow);

        AzkabanEvent origin = new FlowStarted(config, ctx, flow);

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
}
