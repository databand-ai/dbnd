package ai.databand.azkaban;

import ai.databand.azkaban.links.AzkabanLinks;
import ai.databand.azkaban.links.DefaultAzkabanLinks;
import azkaban.executor.ExecutableFlow;
import azkaban.server.AzkabanServer;
import azkaban.utils.Props;

import java.util.Map;

public class EventReporterAzkabanLinks implements AzkabanLinks {

    private final AzkabanLinks origin;

    public EventReporterAzkabanLinks(ExecutableFlow execFlow) {
        Props systemProps = AzkabanServer.getAzkabanProperties();

        String hostName = systemProps.get("server.hostname");
        String port = systemProps.get("server.port");
        String protocol = Boolean.TRUE.toString().equalsIgnoreCase(systemProps.get("jetty.use.ssl")) ? "https" : "http";

        this.origin = new DefaultAzkabanLinks(
            execFlow.getProjectName(),
            execFlow.getId(),
            String.valueOf(execFlow.getExecutionId()),
            protocol,
            hostName,
            port
        );
    }

    public Map<String, String> flowLinks() {
        return origin.flowLinks();
    }

    public Map<String, String> jobLinks(String jobId) {
        return origin.jobLinks(jobId);
    }
}
