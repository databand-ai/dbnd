package ai.databand.azkaban;

import ai.databand.azkaban.links.AzkabanLinks;
import ai.databand.id.Sha1Long;
import ai.databand.id.Sha1Short;
import ai.databand.schema.AzkabanTaskContext;
import ai.databand.schema.LogTarget;
import ai.databand.schema.Pair;
import ai.databand.schema.RunAndDefinition;
import ai.databand.schema.TaskDefinition;
import ai.databand.schema.TaskParamDefinition;
import ai.databand.schema.TaskRun;
import ai.databand.schema.TaskRunParam;
import ai.databand.schema.TaskRunsInfo;
import ai.databand.schema.TrackingSource;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AzkabanFlow {

    private final AzkabanLinks links;
    protected final AzkabanTaskContext azCtx;

    public AzkabanFlow(AzkabanLinks links, AzkabanTaskContext azCtx) {
        this.links = links;
        this.azCtx = azCtx;
    }

    public String uuid() {
        return azCtx.flowUuid();
    }

    public abstract String user();

    public abstract String pipelineName();

    public abstract Map<String, String> flowProps();

    public abstract String envName();

    public abstract String log();

    public abstract String state();

    public abstract ZonedDateTime startDate();

    public abstract boolean isTrack();

    public final TaskRunsInfo toDataband() {
        ZonedDateTime now = startDate();

        String runUid = azCtx.rootRunUid();
        String driverTaskUid = azCtx.driverTaskUid();
        String taskRunEnvUid = azCtx.driverTaskRunEnvUid();
        String taskRunAttemptUid = azCtx.driverTaskRunAttemptUid();
        String cmd = "";
        String version = "";

        Sha1Short taskSignature = new Sha1Short("TASK_SIGNATURE", azCtx.flowUuid());
        String taskDefinitionUid = azCtx.driverTaskDefinitionUid();

        String taskAfId = azCtx.flowId();
        String jobName = azCtx.flowId();

        List<TaskRunParam> params = flowProps()
            .entrySet()
            .stream()
            .map(e -> new TaskRunParam(e.getValue(), "user", e.getKey()))
            .collect(Collectors.toList());

        Pair<List<TaskRunParam>, List<LogTarget>> paramsAndTargets = new Pair<>(params, Collections.emptyList());

        List<TaskParamDefinition> taskParamDefinitions = flowProps()
            .keySet()
            .stream()
            .map(s -> new TaskParamDefinition(
                s,
                "task_input",
                "user",
                true,
                false,
                "string",
                "",
                "")
            )
            .collect(Collectors.toList());

        TaskRun driverTask = new TaskRun(
            runUid,
            true,
            false,
            null,
            version,
            driverTaskUid,
            taskSignature.toString(),
            jobName,
            paramsAndTargets.left(),
            taskSignature.toString(),
            false,
            now.toLocalDate(),
            now,
            "",
            "RUNNING",
            taskDefinitionUid,
            cmd,
            false,
            false,
            taskRunAttemptUid,
            taskAfId,
            true,
            true,
            cmd,
            taskAfId,
            envName(),
            links.flowLinks()
        );
        driverTask.setStartDate(now);

        List<TaskRunsInfo> tasks = tasks();

        List<List<String>> parentChildMap = tasks.stream()
            .flatMap(t -> t.getParentChildMap().stream())
            .collect(Collectors.toList());

        List<TaskRun> taskRuns = tasks.stream()
            .flatMap(t -> t.getTaskRuns().stream())
            .collect(Collectors.toList());
        taskRuns.add(driverTask);

        List<List<String>> upstreamsMap = tasks.stream()
            .flatMap(t -> t.getUpstreamsMap().stream())
            .collect(Collectors.toList());

        List<TaskDefinition> taskDefinitions = tasks.stream()
            .flatMap(t -> t.getTaskDefinitions().stream())
            .collect(Collectors.toList());

        taskDefinitions.add(
            new TaskDefinition(
                azCtx.flowId(),
                null,
                new Sha1Long("SOURCE", azCtx.flowUuid()).toString(),
                "",
                taskDefinitionUid,
                new Sha1Long("MODULE_SOURCE", azCtx.flowUuid()).toString(),
                taskParamDefinitions,
                "jvm_task",
                "java",
                ""
            )
        );

        return new TaskRunsInfo(
            taskRunEnvUid,
            parentChildMap,
            runUid,
            taskRuns,
            Collections.emptyList(),
            runUid,
            upstreamsMap,
            false,
            taskDefinitions,
            new TrackingSource(azCtx)
        );
    }

    public abstract List<Pair<String, Map<String, String>>> jobs();

    public final List<TaskRunsInfo> tasks() {
        return jobs().stream().map(p -> jobToDataband(p.left(), p.right())).collect(Collectors.toList());
    }

    private TaskRunsInfo jobToDataband(String jobId, Map<String, String> props) {
        RunAndDefinition runAndDefinition = buildJobRunAndDefinition(jobId, props);

        TaskRun taskRun = runAndDefinition.taskRun();
        List<TaskRun> taskRuns = Collections.singletonList(taskRun);
        List<TaskDefinition> taskDefinitions = Collections.singletonList(runAndDefinition.taskDefinition());

        List<List<String>> parentChildMap = Collections.singletonList(Arrays.asList(azCtx.driverTaskUid(), taskRun.getTaskRunUid()));
        List<List<String>> upstreamsMap = buildJobUpstreamsMap(jobId, taskRun);

        return new TaskRunsInfo(
            "",
            parentChildMap,
            "",
            taskRuns,
            Collections.emptyList(),
            azCtx.rootRunUid(),
            upstreamsMap,
            true,
            taskDefinitions,
            new TrackingSource(azCtx)
        );

    }

    protected abstract List<List<String>> buildJobUpstreamsMap(String jobId, TaskRun taskRun);

    protected final RunAndDefinition buildJobRunAndDefinition(String jobId, Map<String, String> props) {
        AzkabanTaskContext jobCtx = azCtx.forJob(jobId);
        String cmd = "";
        if (props.containsKey("command")) {
            cmd = props.get("command");
        } else if (props.containsKey("job.class")) {
            cmd = props.get("job.class");
        } else if (props.containsKey("java.class")) {
            cmd = props.get("java.class");
        }

        List<TaskParamDefinition> paramDefinitions = props
            .keySet()
            .stream()
            .map(s -> new TaskParamDefinition(
                s,
                "task_input",
                "user",
                true,
                false,
                "string",
                "",
                "")
            )
            .collect(Collectors.toList());

        String taskRunUid = jobCtx.taskRunUid();

        String taskSignature = new Sha1Short("TASK_SIGNATURE" + jobId, jobCtx.flowUuid()).toString();

        String taskDefinitionUid = jobCtx.taskDefinitionUid();
        String taskRunAttemptUid = jobCtx.taskRunAttemptUid();

        String taskAfId = jobId;

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        List<LogTarget> targets = Collections.emptyList();

        List<TaskRunParam> params = props
            .entrySet()
            .stream()
            .map(e -> new TaskRunParam(e.getValue(), "", e.getKey()))
            .collect(Collectors.toList());

        TaskRun taskRun = new TaskRun(
            jobCtx.rootRunUid(),
            false,
            false,
            null,
            "",
            taskRunUid,
            taskSignature,
            taskAfId,
            params,
            taskSignature,
            false,
            now.toLocalDate(),
            now,
            "",
            "QUEUED",
            taskDefinitionUid,
            cmd,
            false,
            false,
            taskRunAttemptUid,
            taskAfId,
            false,
            false,
            jobId,
            taskAfId,
            "jvm",
            links.jobLinks(jobId)
        );

        TaskDefinition taskDefinition = new TaskDefinition(
            jobId,
            "",
            new Sha1Long("SOURCE", jobCtx.flowUuid()).toString(),
            "",
            taskDefinitionUid,
            new Sha1Long("MODULE_SOURCE", jobCtx.flowUuid()).toString(),
            paramDefinitions,
            "jvm_task",
            "java",
            ""
        );

        return new RunAndDefinition(taskRun, taskDefinition, targets);
    }
}
