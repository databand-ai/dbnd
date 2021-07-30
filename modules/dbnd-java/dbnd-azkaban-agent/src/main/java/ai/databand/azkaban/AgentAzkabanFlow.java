package ai.databand.azkaban;

import ai.databand.azkaban.events.FlowRunnerContext;
import ai.databand.config.DbndConfig;
import ai.databand.id.Uuid5;
import ai.databand.log.TruncatedLog;
import ai.databand.schema.Pair;
import ai.databand.schema.TaskRun;
import azkaban.execapp.FlowRunner;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;
import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.utils.Props;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AgentAzkabanFlow extends AzkabanFlow {

    private final FlowRunnerContext ctx;
    private final Map<String, String> flowProps;
    private final DbndConfig config;
    private final FlowRunner flowRunner;
    private final Flow flow;
    private final ExecutableFlow executableFlow;

    public AgentAzkabanFlow(DbndConfig config, FlowRunnerContext ctx) {
        super(ctx.links(), ctx.taskContext());
        this.ctx = ctx;
        this.config = config;
        this.flowRunner = ctx.flowRunner();
        this.executableFlow = ctx.executableFlow();
        this.flow = ctx.flowDef();
        this.flowProps = executableFlow.getInputProps().getFlattened();
    }

    @Override
    public boolean isTrack() {
        AzkabanDbndConfig azConfig = new AzkabanDbndConfig(config);
        return azConfig.isTrackingEnabled(ctx.taskContext());
    }

    @Override
    public String user() {
        return executableFlow.getSubmitUser();
    }

    @Override
    public String pipelineName() {
        return ctx.pipelineName();
    }

    @Override
    public Map<String, String> flowProps() {
        return flowProps;
    }

    @Override
    public String envName() {
        return ctx.envName();
    }

    @Override
    public String log() {
        File logFile = flowRunner.getFlowLogFile();
        return new TruncatedLog(config, logFile).toString();
    }

    @Override
    public String state() {
        if (Status.KILLED == executableFlow.getStatus() || Status.CANCELLED == executableFlow.getStatus()) {
            return "CANCELLED";
        }
        return Status.SUCCEEDED == executableFlow.getStatus() ? "SUCCESS" : "FAILED";
    }

    @Override
    public ZonedDateTime startDate() {
        return Instant.ofEpochMilli(Long.parseLong(ctx.startTime())).atZone(ZoneOffset.UTC);
    }

    @Override
    public List<Pair<String, Map<String, String>>> jobs() {
        List<Pair<String, Map<String, String>>> result = new ArrayList<>(1);
        try {
            Method method = flowRunner.getClass().getDeclaredMethod("loadJobProps", ExecutableNode.class);
            method.setAccessible(true);

            for (Node node : flow.getNodes()) {
                Props props = (Props) method.invoke(flowRunner, executableFlow.getExecutableNode(node.getId()));
                result.add(new Pair<>(node.getId(), props.getFlattened()));
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
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
