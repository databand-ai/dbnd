/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban.links;

import java.util.HashMap;
import java.util.Map;

public class DefaultAzkabanLinks implements AzkabanLinks {

    private final String url;
    private final String projectName;
    private final String flowName;
    private final String executionId;

    public DefaultAzkabanLinks(String projectName,
                               String flowName,
                               String executionId,
                               String protocol,
                               String hostName,
                               String port) {
        this.projectName = projectName;
        this.flowName = flowName;
        this.executionId = executionId;

        this.url = String.format("%s://%s:%s", protocol, hostName, port);
    }

    public String executionLink() {
        return String.format("%s/executor?execid=%s", url, executionId);
    }

    public String jobLogsLink(String jobId) {
        if (jobId.isEmpty()) {
            return executionLink();
        }
        return String.format("%s/executor?execid=%s&job=%s", url, executionId, jobId);
    }

    public String flowLink() {
        if (this.flowName.isEmpty()) {
            return url;
        }
        return String.format("%s/manager?project=%s&flow=%s", url, projectName, flowName);
    }

    @Override
    public Map<String, String> flowLinks() {
        Map<String, String> links = new HashMap<>(2);
        links.put("azkaban flow", flowLink());
        links.put("azkaban flow execution", executionLink());
        return links;
    }

    @Override
    public Map<String, String> jobLinks(String jobId) {
        Map<String, String> links = new HashMap<>(2);
        links.put("azkaban flow execution", executionLink());
        links.put("azkaban job logs", jobLogsLink(jobId));
        return links;
    }
}
