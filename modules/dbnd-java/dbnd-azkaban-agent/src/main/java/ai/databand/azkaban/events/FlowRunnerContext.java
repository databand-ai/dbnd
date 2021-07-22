package ai.databand.azkaban.events;

import ai.databand.azkaban.AgentAzkabanLinks;
import ai.databand.azkaban.links.AzkabanLinks;
import ai.databand.config.DbndConfig;
import ai.databand.schema.AzkabanTaskContext;
import azkaban.event.Event;
import azkaban.execapp.FlowRunner;
import azkaban.executor.ExecutableFlow;
import azkaban.flow.CommonJobProperties;
import azkaban.flow.Flow;
import azkaban.project.JdbcProjectLoader;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.server.AzkabanServer;
import azkaban.utils.Props;

public class FlowRunnerContext {

    private final ExecutableFlow executableFlow;
    private final FlowRunner flowRunner;
    private final Flow flowDef;
    private final String flowId;
    private final String projectName;
    private final String flowUuid;
    private final String executionId;
    private final String startTime;
    private final String pipelineName;
    private final DbndConfig config;

    public FlowRunnerContext(Event event, DbndConfig config) {
        this.flowRunner = (FlowRunner) event.getRunner();
        this.executableFlow = flowRunner.getExecutableFlow();
        this.flowId = executableFlow.getId();
        this.projectName = executableFlow.getProjectName();

        this.flowUuid = executableFlow.getInputProps().get(CommonJobProperties.FLOW_UUID);
        this.executionId = String.valueOf(executableFlow.getExecutionId());
        this.startTime = String.valueOf(event.getTime());

        ProjectManager projectManager = new ProjectManager(new JdbcProjectLoader(AzkabanServer.getAzkabanProperties()), AzkabanServer.getAzkabanProperties());
        Project project = projectManager.getProject(projectName);
        this.flowDef = project.getFlow(flowId);

        this.pipelineName = String.format("%s__%s", projectName, flowId);
        this.config = config;
    }

    public ExecutableFlow executableFlow() {
        return executableFlow;
    }

    public FlowRunner flowRunner() {
        return flowRunner;
    }

    public String startTime() {
        return startTime;
    }

    public AzkabanTaskContext taskContext() {
        return new AzkabanTaskContext(projectName, flowId, flowUuid, executionId, "", config);
    }

    public AzkabanLinks links() {
        return new AgentAzkabanLinks(executableFlow);
    }

    public Flow flowDef() {
        return flowDef;
    }

    public String pipelineName() {
        return pipelineName;
    }

    public String envName() {
        Props systemProps = AzkabanServer.getAzkabanProperties();
        return String.format("%s: %s", systemProps.get("azkaban.name"), systemProps.get("azkaban.label"));
    }

    public boolean isTrack() {
        return false;
    }
}
