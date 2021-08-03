package ai.databand.azkaban;

import ai.databand.azkaban.links.AzkabanLinks;
import ai.databand.azkaban.links.DefaultAzkabanLinks;
import ai.databand.azkaban.links.EmptyAzkabanLinks;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.project.ProjectManager;
import azkaban.webapp.AzkabanWebServer;

import java.util.Map;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

public class ReporterAzkabanLinks implements AzkabanLinks {

    private final AzkabanLinks origin;

    public ReporterAzkabanLinks(String executionId) {
        AzkabanWebServer webServer = SERVICE_PROVIDER.getInstance(AzkabanWebServer.class);
        ProjectManager projectManager = webServer.getProjectManager();
        ExecutorManagerAdapter executor = webServer.getExecutorManager();

        String hostName = projectManager.getProps().get("server.hostname");
        String port = projectManager.getProps().get("server.port");
        String protocol = Boolean.TRUE.toString().equalsIgnoreCase(projectManager.getProps().get("jetty.use.ssl")) ? "https" : "http";

        ExecutableFlow execFlow;
        String projectName = "";
        String flowName = "";

        try {
            execFlow = executor.getExecutableFlow(Integer.parseInt(executionId));
            projectName = execFlow.getProjectName();
            flowName = execFlow.getFlowName();
        } catch (ExecutorManagerException e) {
            this.origin = new EmptyAzkabanLinks();
            return;
        }

        this.origin = new DefaultAzkabanLinks(
            projectName,
            flowName,
            executionId,
            protocol,
            hostName,
            port
        );
    }

    @Override
    public Map<String, String> flowLinks() {
        return origin.flowLinks();
    }

    @Override
    public Map<String, String> jobLinks(String jobId) {
        return origin.jobLinks(jobId);
    }
}
