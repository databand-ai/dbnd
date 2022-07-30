/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban;

import ai.databand.azkaban.links.AzkabanLinks;
import ai.databand.config.DbndConfig;
import ai.databand.id.Uuid5;
import ai.databand.schema.AzkabanTaskContext;
import ai.databand.schema.Pair;
import ai.databand.schema.TaskRun;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

public class EventReporterAzkabanFlow extends AzkabanFlow {

    private final DbndConfig config;
    private final ExecutableFlow executableFlow;
    private final Project project;
    private final Flow flow;


    public EventReporterAzkabanFlow(DbndConfig config,
                                    AzkabanLinks links,
                                    AzkabanTaskContext azCtx,
                                    ExecutableFlow executableFlow) {
        super(links, azCtx);
        this.config = config;
        this.executableFlow = executableFlow;

        ProjectManager projectManager = SERVICE_PROVIDER.getInstance(ProjectManager.class);

        this.project = projectManager.getProject(executableFlow.getProjectName());
        this.flow = project.getFlow(executableFlow.getFlowId());
    }

    @Override
    public String user() {
        return executableFlow.getSubmitUser();
    }

    @Override
    public String pipelineName() {
        return null;
    }

    @Override
    public Map<String, String> flowProps() {
        return executableFlow.getInputProps().getFlattened();
    }

    @Override
    public String envName() {
        return null;
    }

    @Override
    public String log() {
        AzkabanWebServer webServer = SERVICE_PROVIDER.getInstance(AzkabanWebServer.class);
        ExecutorManagerAdapter executorManagerAdapter = webServer.getExecutorManager();
        try {
            // todo: executor manager adapter doesn't allow to fetch log size and we cannot determine how many bytes to read
            final FileIOUtils.LogData data = executorManagerAdapter.getExecutableFlowLog(executableFlow, 0, config.previewTotalBytes());
            return data.getData();
        } catch (ExecutorManagerException e) {
            return "";
        }
    }

    @Override
    public String state() {
        return Status.FAILED == executableFlow.getStatus() ? "FAILED" : "SUCCESS";
    }

    @Override
    public ZonedDateTime startDate() {
        return null;
    }

    @Override
    public boolean isTrack() {
        AzkabanDbndConfig azConfig = new AzkabanDbndConfig(config);
        return azConfig.isTrackingEnabled(azCtx);
    }

    @Override
    public List<Pair<String, Map<String, String>>> jobs() {
        List<Pair<String, Map<String, String>>> result = new ArrayList<>(1);
        ProjectManager projectManager = SERVICE_PROVIDER.getInstance(ProjectManager.class);

        for (Node node : flow.getNodes()) {
            Props props = projectManager.getJobOverrideProperty(project, flow, node.getId(), node.getJobSource());
            result.add(new Pair<>(node.getId(), props.getFlattened()));
        }
        return result;
    }

    @Override
    protected List<List<String>> buildJobUpstreamsMap(String jobId, TaskRun taskRun) {
        Set<Edge> inEdges = flow.getInEdges(jobId);
        List<List<String>> upstreamsMap = new ArrayList<>(1);

        if (inEdges != null && !inEdges.isEmpty()) {
            for (Edge edge : inEdges) {
                String upstreamTaskRunUid = new Uuid5("TASK_RUN_UID", edge.getSourceId() + azCtx.flowUuid()).toString();
                upstreamsMap.add(Arrays.asList(taskRun.getTaskRunUid(), upstreamTaskRunUid));
            }
        }
        return upstreamsMap;
    }
}
