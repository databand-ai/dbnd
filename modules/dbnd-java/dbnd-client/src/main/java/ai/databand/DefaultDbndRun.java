/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand;

import ai.databand.config.DbndConfig;
import ai.databand.id.Sha1Long;
import ai.databand.id.Sha1Short;
import ai.databand.id.Uuid5;
import ai.databand.log.HistogramRequest;
import ai.databand.log.LogDatasetRequest;
import ai.databand.parameters.DatasetOperationPreview;
import ai.databand.parameters.Histogram;
import ai.databand.parameters.NullPreview;
import ai.databand.parameters.ParametersPreview;
import ai.databand.parameters.TaskParameterPreview;
import ai.databand.schema.AirflowTaskContext;
import ai.databand.schema.AzkabanTaskContext;
import ai.databand.schema.ColumnStats;
import ai.databand.schema.DatasetOperationStatus;
import ai.databand.schema.DatasetOperationType;
import ai.databand.schema.ErrorInfo;
import ai.databand.schema.LogDataset;
import ai.databand.schema.LogTarget;
import ai.databand.schema.Pair;
import ai.databand.schema.RootRun;
import ai.databand.schema.RunAndDefinition;
import ai.databand.schema.TaskDefinition;
import ai.databand.schema.TaskParamDefinition;
import ai.databand.schema.TaskRun;
import ai.databand.schema.TaskRunParam;
import ai.databand.schema.TaskRunsInfo;
import ai.databand.schema.TrackingSource;
import ai.databand.spark.SparkColumnStats;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@SuppressWarnings("unchecked")
public class DefaultDbndRun implements DbndRun {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDbndRun.class);

    private final DbndClient dbnd;
    private final List<TaskRun> taskRuns;
    private final List<TaskDefinition> taskDefinitions;
    private final List<List<String>> parentChildMap;
    private final List<List<String>> upstreamsMap;
    private final Deque<TaskRun> stack;
    // todo: methods cache should be extracted to app-level cache
    private final Map<Method, List<TaskParamDefinition>> methodsCache;
    private final Map<Method, TaskRun> methodsRunsCache;
    private final Map<Method, Integer> methodExecutionCounts;
    private final ParametersPreview parameters;
    private final Map<Integer, TaskRun> taskRunOutputs;
    private String rootRunUid;
    private String runId;
    private String jobName;
    private String driverTaskUid;
    private TaskRun driverTask;
    private AirflowTaskContext airflowContext;
    private AzkabanTaskContext azkabanTaskContext;
    private final DbndConfig config;

    public DefaultDbndRun(DbndClient dbndClient, DbndConfig config) {
        this.dbnd = dbndClient;
        this.taskRuns = new ArrayList<>(1);
        this.taskDefinitions = new ArrayList<>(1);
        this.parentChildMap = new ArrayList<>(1);
        this.upstreamsMap = new ArrayList<>(1);
        this.stack = new ArrayDeque<>(1);
        this.methodsCache = new HashMap<>(1);
        this.methodsRunsCache = new HashMap<>(1);
        this.methodExecutionCounts = new HashMap<>(1);
        this.parameters = new ParametersPreview(config.isPreviewEnabled());
        this.taskRunOutputs = new HashMap<>(1);
        this.airflowContext = config.airflowContext().orElse(null);
        this.azkabanTaskContext = config.azkabanContext().orElse(null);
        this.config = config;
    }

    @Override
    public void init(Method method, Object[] args) {
        String annotationValue = getTaskName(method);
        this.runId = UUID.randomUUID().toString();
        String user = System.getProperty("user.name");
        String source = null;
        TrackingSource trackingSource = null;
        if (airflowContext != null) {
            this.jobName = airflowContext.jobName();
            source = "airflow_tracking";
            trackingSource = new TrackingSource(airflowContext);
        } else if (azkabanTaskContext != null) {
            this.jobName = azkabanTaskContext.databandJobName();
            trackingSource = azkabanTaskContext.trackingSource();
            if (trackingSource != null) {
                source = "azkaban_tracking";
            }
        } else {
            this.jobName = annotationValue == null || annotationValue.isEmpty() ? method.getName() : annotationValue;
        }
        config.jobName().ifPresent(name -> this.jobName = name);
        TaskRunsInfo rootRun = buildRootRun(method, args);
        RootRun root = config.azkabanContext().isPresent() ? config.azkabanContext().get().root() : null;
        this.rootRunUid = dbnd.initRun(jobName, runId, user, config.runName(), rootRun, airflowContext, root, source, trackingSource, null);
        dbnd.setRunState(this.rootRunUid, "RUNNING");
    }

    /**
     * Builds root run.
     *
     * @return
     */
    protected TaskRunsInfo buildRootRun(Method method, Object[] args) {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String runUid = new Uuid5("RUN_UID", runId).toString();
        driverTaskUid = new Uuid5("DRIVER_TASK", runId).toString();
        String taskRunEnvUid = new Uuid5("TASK_RUN_ENV_UID", runId).toString();
        String taskRunAttemptUid = new Uuid5("TASK_RUN_ATTEMPT", runId).toString();
        String cmd = config.cmd();
        String version = "";

        Sha1Short taskSignature = new Sha1Short("TASK_SIGNATURE", runId);
        String taskDefinitionUid = new Uuid5("TASK_DEFINITION", runId).toString();

        String taskAfId = getTaskName(method);

        List<TaskParamDefinition> taskParamDefinitions = buildTaskParamDefinitions(method);
        String methodName = method == null ? "pipeline" : method.getName();
        Pair<List<TaskRunParam>, List<LogTarget>> paramsAndTargets = buildTaskRunParamsAndTargets(
            method,
            args,
            runUid,
            methodName,
            taskRunAttemptUid,
            taskDefinitionUid
        );

        this.driverTask = new TaskRun(
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
            airflowContext != null || azkabanTaskContext != null,
            true,
            cmd,
            taskAfId,
            "jvm",
            Collections.emptyMap()
        );
        this.driverTask.setStartDate(now);

        String sourceCode = extractSourceCode(method);

        TrackingSource trackingSource = null;

        if (airflowContext != null) {
            this.parentChildMap.add(Arrays.asList(airflowContext.getAfOperatorUid(), driverTaskUid));
            this.upstreamsMap.add(Arrays.asList(airflowContext.getAfOperatorUid(), driverTaskUid));
            trackingSource = new TrackingSource(airflowContext);
        }

        if (azkabanTaskContext != null) {
            this.parentChildMap.add(Arrays.asList(azkabanTaskContext.taskRunUid(), driverTaskUid));
            this.upstreamsMap.add(Arrays.asList(azkabanTaskContext.taskRunUid(), driverTaskUid));
            trackingSource = new TrackingSource(azkabanTaskContext);
        }

        return new TaskRunsInfo(
            taskRunEnvUid,
            parentChildMap,
            runUid,
            Collections.singletonList(driverTask),
            Collections.emptyList(),
            runUid,
            upstreamsMap,
            false,
            Collections.singletonList(
                new TaskDefinition(
                    methodName,
                    sourceCode,
                    new Sha1Long("SOURCE", runId).toString(),
                    "",
                    taskDefinitionUid,
                    new Sha1Long("MODULE_SOURCE", runId).toString(),
                    taskParamDefinitions,
                    "jvm_task",
                    "java",
                    ""
                )
            ),
            trackingSource
        );
    }

    // TODO: actual source code
    protected String extractSourceCode(Method method) {
        return "";
    }

    @Override
    public void startTask(Method method, Object[] args) {
        RunAndDefinition runAndDefinition = buildRunAndDefinition(method, args, !stack.isEmpty());

        TaskRun taskRun = runAndDefinition.taskRun();
        taskRuns.add(taskRun);

        TaskDefinition taskDefinition = runAndDefinition.taskDefinition();
        taskDefinitions.add(taskDefinition);

        TaskRun parent = stack.isEmpty() ? driverTask : stack.peek();

        // detect nested tasks
        if (!stack.isEmpty()) {
            upstreamsMap.add(Arrays.asList(parent.getTaskRunUid(), taskRun.getTaskRunUid()));
        }
        taskRun.addUpstream(parent);

        // detect upstream-downstream relations
        for (Object arg : args) {
            if (arg == null) {
                continue;
            }
            TaskRun parentTask = taskRunOutputs.get(arg.hashCode());
            if (parentTask != null) {
                upstreamsMap.add(Arrays.asList(taskRun.getTaskRunUid(), parentTask.getTaskRunUid()));
            }
        }

        stack.push(taskRun);

        parentChildMap.add(Arrays.asList(parent.getTaskRunUid(), taskRun.getTaskRunUid()));

        dbnd.addTaskRuns(rootRunUid, runId, taskRuns, taskDefinitions, parentChildMap, upstreamsMap);
        dbnd.logTargets(taskRun, runAndDefinition.targets());
        dbnd.updateTaskRunAttempt(taskRun.getTaskRunUid(), taskRun.getTaskRunAttemptUid(), "RUNNING", null, taskRun.getStartDate());
        LOG.info("[task_run_uid: {}, task_name: {}] task tracker url: {}/app/jobs/{}/{}/{}",
            taskRun.getTaskRunUid(),
            taskRun.getTaskId(),
            config.databandUrl(),
            this.driverTask.getTaskAfId(),
            this.driverTask.getRunUid(),
            taskRun.getTaskRunUid()
        );
    }

    protected List<TaskParamDefinition> buildTaskParamDefinitions(Method method) {
        if (method == null) {
            return Collections.emptyList();
        }
        return methodsCache.computeIfAbsent(method, method1 -> {
            List<TaskParamDefinition> result = new ArrayList<>(method.getParameterCount());
            for (int i = 0; i < method.getParameterCount(); i++) {
                Parameter parameter = method.getParameters()[i];
                result.add(
                    new TaskParamDefinition(
                        parameter.getName(),
                        "task_input",
                        "user",
                        true,
                        false,
                        parameter.getParameterizedType().getTypeName(),
                        "",
                        ""
                    )
                );
            }
            result.add(
                new TaskParamDefinition(
                    "result",
                    "task_output",
                    "user",
                    true,
                    false,
                    method.getReturnType().getTypeName(),
                    "",
                    ""
                )
            );
            return result;
        });
    }

    protected Pair<List<TaskRunParam>, List<LogTarget>> buildTaskRunParamsAndTargets(Method method,
                                                                                     Object[] args,
                                                                                     String taskRunUid,
                                                                                     String methodName,
                                                                                     String taskRunAttemptUid,
                                                                                     String taskDefinitionUid) {
        if (method == null || args == null || args.length == 0) {
            return new Pair<>(Collections.emptyList(), Collections.emptyList());
        }
        List<LogTarget> targets = new ArrayList<>(1);
        List<TaskRunParam> params = new ArrayList<>(method.getParameterCount());

        for (int i = 0; i < method.getParameterCount(); i++) {
            Parameter parameter = method.getParameters()[i];

            Object parameterValue = args[i];

            TaskParameterPreview preview = parameters.get(parameter.getType());
            String compactPreview = preview.compact(parameterValue);
            params.add(
                new TaskRunParam(
                    compactPreview,
                    "",
                    parameter.getName()
                )
            );

            String targetPath = String.format("%s.%s", method.getName(), parameter.getName());

            targets.add(
                new LogTarget(
                    rootRunUid,
                    taskRunUid,
                    methodName,
                    taskRunAttemptUid,
                    targetPath,
                    parameter.getName(),
                    taskDefinitionUid,
                    "read",
                    "OK",
                    preview.full(parameterValue),
                    preview.dimensions(parameterValue),
                    preview.schema(parameterValue),
                    new Sha1Long("", compactPreview).toString()
                )
            );
        }

        TaskParameterPreview resultPreview = parameters.get(method.getReturnType());

        params.add(
            new TaskRunParam(
                resultPreview.typeName(method.getReturnType()),
                "",
                "result"
            )
        );
        return new Pair<>(params, targets);
    }

    public String getTaskName(Method method) {
        if (method == null || method.getName().contains("$anon")) {
            // we're running from spark-submit
            return config.sparkAppName();
        }
        Optional<Annotation> taskAnnotation = Arrays.stream(method.getAnnotations())
            .filter(at -> at.toString().contains("ai.databand.annotations.Task(value="))
            .findAny();
        if (!taskAnnotation.isPresent()) {
            return method.getName();
        }
        String annotationStr = taskAnnotation.get().toString();
        String annotationValue = annotationStr.substring(annotationStr.indexOf('=') + 1, annotationStr.indexOf(')'));
        return annotationValue.isEmpty() ? method.getName() : annotationValue;
    }

    protected RunAndDefinition buildRunAndDefinition(Method method, Object[] args, boolean hasUpstreams) {
        int executionCount = methodExecutionCounts.computeIfAbsent(method, m -> 0);
        executionCount++;

        String taskName = getTaskName(method);
        String methodName = executionCount == 1 ? taskName : String.format("%s_%s", taskName, executionCount);
        methodExecutionCounts.put(method, executionCount);

        List<TaskParamDefinition> paramDefinitions = buildTaskParamDefinitions(method);

        String taskRunId = UUID.randomUUID().toString();
        String taskRunUid = new Uuid5("TASK_RUN_UID", taskRunId).toString();

        String taskSignature = new Sha1Short("TASK_SIGNATURE" + methodName, runId).toString();

        String taskDefinitionUid = new Uuid5("TASK_DEFINITION" + methodName, runId).toString();
        String taskRunAttemptUid = new Uuid5("TASK_RUN_ATTEMPT" + methodName, runId).toString();

        String taskAfId = methodName;

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        Pair<List<TaskRunParam>, List<LogTarget>> paramsAndTargets = buildTaskRunParamsAndTargets(
            method,
            args,
            taskRunUid,
            methodName,
            taskRunAttemptUid,
            taskDefinitionUid
        );

        List<TaskRunParam> params = paramsAndTargets.left();
        List<LogTarget> targets = paramsAndTargets.right();

        TaskRun taskRun = new TaskRun(
            rootRunUid,
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
            methodName,
            false,
            hasUpstreams,
            taskRunAttemptUid,
            taskAfId,
            airflowContext != null,
            false,
            methodName,
            taskAfId,
            "jvm",
            Collections.emptyMap()
        );

        TaskDefinition taskDefinition = new TaskDefinition(
            methodName,
            "",
            new Sha1Long("SOURCE", runId).toString(),
            "",
            taskDefinitionUid,
            new Sha1Long("MODULE_SOURCE", runId).toString(),
            paramDefinitions,
            "jvm_task",
            "java",
            ""
        );

        methodsRunsCache.put(method, taskRun);

        return new RunAndDefinition(taskRun, taskDefinition, targets);
    }

    @Override
    public void errorTask(Method method, Throwable error) {
        TaskRun task = stack.pop();

        if (task == null) {
            return;
        }

        String stackTrace = extractStackTrace(error);
        task.appendLog(stackTrace);
        dbnd.saveTaskLog(task.getTaskRunUid(), task.getTaskRunAttemptUid(), task.getTaskLog());
        dbnd.logMetrics(task, task.getMetrics(), "spark");
        ErrorInfo errorInfo = new ErrorInfo(
            error.getLocalizedMessage(),
            "",
            false,
            stackTrace,
            "",
            "",
            false,
            error.getClass().getCanonicalName()
        );
        dbnd.updateTaskRunAttempt(task.getTaskRunUid(), task.getTaskRunAttemptUid(), "FAILED", errorInfo, task.getStartDate());
    }

    protected String extractStackTrace(Throwable error) {
        try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw)) {
            error.printStackTrace(pw);
            return sw.toString();
        } catch (IOException e) {
            LOG.error("Unable to extract stack trace from error", e);
            return "";
        }
    }

    @Override
    public void completeTask(Method method, Object result) {
        TaskRun task = stack.pop();
        if (task == null) {
            return;
        }
        if (result != null) {
            TaskParameterPreview taskParameter = parameters.get(result.getClass());
            String preview = taskParameter.full(result);
            taskRunOutputs.put(result.hashCode(), task);
            dbnd.logTargets(
                task,
                Collections.singletonList(
                    new LogTarget(
                        rootRunUid,
                        task.getTaskRunUid(),
                        task.getTaskAfId(),
                        task.getTaskRunAttemptUid(),
                        new Sha1Long("TARGET_PATH", preview).toString(),
                        "result",
                        task.getTaskDefinitionUid(),
                        "write",
                        "OK",
                        preview,
                        taskParameter.dimensions(result),
                        taskParameter.schema(result),
                        new Sha1Long("", preview).toString()
                    )
                ));
        }
        dbnd.saveTaskLog(task.getTaskRunUid(), task.getTaskRunAttemptUid(), task.getTaskLog());
        dbnd.logMetrics(task, task.getMetrics(), "spark");
        dbnd.updateTaskRunAttempt(task.getTaskRunUid(), task.getTaskRunAttemptUid(), "SUCCESS", null, task.getStartDate());
    }

    @Override
    public void stop() {
        dbnd.saveTaskLog(driverTask.getTaskRunUid(), driverTask.getTaskRunAttemptUid(), driverTask.getTaskLog());
        dbnd.logMetrics(driverTask, driverTask.getMetrics(), "spark");
        dbnd.updateTaskRunAttempt(driverTask.getTaskRunUid(), driverTask.getTaskRunAttemptUid(), "SUCCESS", null, driverTask.getStartDate());
        if (rootRunUid == null) {
            // for agentless runs created inside Databand Context (when root run is outside of JVM) we shouldn't complete run
            return;
        }
        dbnd.setRunState(rootRunUid, "SUCCESS");
    }

    @Override
    public void stopExternal() {
        if (driverTask == null) {
            return;
        }
        dbnd.saveTaskLog(driverTask.getTaskRunUid(), driverTask.getTaskRunAttemptUid(), driverTask.getTaskLog());
        dbnd.logMetrics(driverTask, driverTask.getMetrics(), "spark");
    }

    public void error(Throwable error) {
        String stackTrace = extractStackTrace(error);
        ErrorInfo errorInfo = new ErrorInfo(
            error.getLocalizedMessage(),
            "",
            false,
            stackTrace,
            "",
            "",
            false,
            error.getClass().getCanonicalName()
        );
        driverTask.appendLog(stackTrace);
        dbnd.saveTaskLog(driverTask.getTaskRunUid(), driverTask.getTaskRunAttemptUid(), driverTask.getTaskLog());
        dbnd.logMetrics(driverTask, driverTask.getMetrics(), "spark");
        dbnd.updateTaskRunAttempt(driverTask.getTaskRunUid(), driverTask.getTaskRunAttemptUid(), "FAILED", errorInfo, driverTask.getStartDate());
        dbnd.setRunState(rootRunUid, "FAILED");
    }

    @Override
    public void logMetric(String key, Object value) {
        TaskRun currentTask = stack.peek();
        if (currentTask == null) {
            currentTask = driverTask;
        }
        this.logMetric(currentTask, key, value, null);
    }

    @Override
    public void logMetrics(Map<String, Object> metrics) {
        this.logMetrics(metrics, null);
    }

    @Override
    public void logMetrics(Map<String, Object> metrics, String source) {
        TaskRun currentTask = stack.peek();
        if (currentTask == null) {
            currentTask = driverTask;
        }
        this.logMetrics(currentTask, metrics, source);
    }

    @Override
    public void logDataframe(String key, Dataset<?> value, HistogramRequest histogramRequest) {
        try {
            TaskRun currentTask = stack.peek();
            if (currentTask == null) {
                currentTask = driverTask;
            }
            logMetric(currentTask, key, value, "user", false);
            dbnd.logMetrics(currentTask, new Histogram(key, value, histogramRequest).metricValues(), "histograms");
        } catch (Exception e) {
            LOG.error("Unable to log dataframe", e);
        }
    }

    @Override
    public void logHistogram(Map<String, Object> histogram) {
        try {
            TaskRun currentTask = stack.peek();
            if (currentTask == null) {
                currentTask = driverTask;
            }
            dbnd.logMetrics(currentTask, histogram, "histograms");
        } catch (Exception e) {
            LOG.error("Unable to log histogram", e);
        }
    }

    @Override
    public void logDatasetOperation(String path,
                                    DatasetOperationType type,
                                    DatasetOperationStatus status,
                                    String error,
                                    String valuePreview,
                                    List<Long> dataDimensions,
                                    Object dataSchema,
                                    Boolean withPartition,
                                    List<ColumnStats> columnStats,
                                    String operationSource) {
        try {
            TaskRun currentTask = stack.peek();
            if (currentTask == null) {
                currentTask = driverTask;
            }
            dbnd.logDatasetOperations(currentTask,
                Collections.singletonList(
                    new LogDataset(
                        currentTask,
                        path,
                        type,
                        status,
                        error,
                        valuePreview,
                        dataDimensions,
                        dataSchema,
                        withPartition,
                        columnStats,
                        operationSource
                    )
                ));
        } catch (Exception e) {
            LOG.error("Unable to log dataset operation", e);
        }
    }

    @Override
    public void logDatasetOperation(String path,
                                    DatasetOperationType type,
                                    DatasetOperationStatus status,
                                    Dataset<?> data,
                                    Throwable error,
                                    LogDatasetRequest params,
                                    String operationSource) {
        TaskParameterPreview preview = params.getWithSchema() ? new DatasetOperationPreview() : new NullPreview();
        String errorStr = null;
        if (error != null) {
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                error.printStackTrace(pw);
                errorStr = sw.toString();
            }
        }
        SparkColumnStats columnStats = new SparkColumnStats(data, params);
        logDatasetOperation(
            path,
            type,
            status,
            errorStr,
            preview.full(data),
            preview.dimensions(data),
            preview.schema(data),
            params.getWithPartition(),
            columnStats.values(),
            operationSource
        );
    }

    public void logMetric(TaskRun taskRun, String key, Object value, String source) {
        logMetric(taskRun, key, value, source, true);
    }

    public void logMetric(TaskRun taskRun, String key, Object value, String source, boolean compact) {
        try {
            if (taskRun == null) {
                return;
            }
            TaskParameterPreview taskParameter = parameters.get(value.getClass());
            dbnd.logMetric(
                taskRun,
                key,
                compact ? taskParameter.compact(value) : taskParameter.full(value),
                source
            );
        } catch (Exception e) {
            LOG.error("Unable to log metric", e);
        }
    }

    public void logMetrics(TaskRun taskRun, Map<String, Object> metrics, String source) {
        try {
            if (taskRun == null) {
                return;
            }
            Map<String, Object> result = new HashMap<>(metrics.size());
            for (Map.Entry<String, Object> entry : metrics.entrySet()) {
                TaskParameterPreview taskParameter = parameters.get(entry.getValue().getClass());
                result.put(entry.getKey(), taskParameter.compact(entry.getValue()));
            }
            dbnd.logMetrics(taskRun, result, source);
        } catch (Exception e) {
            LOG.error("Unable to log metrics");
        }
    }

    @Override
    public void saveLog(LoggingEvent event, String formattedEvent) {
        try {
            if (driverTask == null) {
                return;
            }
            TaskRun currentTask = stack.peek();
            // TODO: filter out unrelated messages
            if (DbndClient.class.getName().equals(event.getLoggerName())) {
                return;
            }
            if (currentTask == null) {
                driverTask.appendLog(formattedEvent);
            } else {
                currentTask.appendLog(formattedEvent);
            }
        } catch (Exception e) {
            LOG.error("Unable to save task log", e);
        }
    }

    @Override
    public void saveSparkMetrics(SparkListenerStageCompleted event) {
        try {
            StageInfo stageInfo = event.stageInfo();
            TaskRun currentTask = stack.peek();
            if (currentTask == null) {
                currentTask = driverTask;
            }
            String transformationName = stageInfo.name().substring(0, stageInfo.name().indexOf(' '));
            String metricPrefix = String.format("stage-%s.%s.", stageInfo.stageId(), transformationName);

            Iterator<AccumulatorV2<?, ?>> it = stageInfo.taskMetrics().accumulators().iterator();
            Map<String, Object> values = new HashMap<>(1);
            Map<String, Object> prefixedValues = new HashMap<>(1);
            while (it.hasNext()) {
                AccumulatorV2<?, ?> next = it.next();
                // we're capturing only numeric values
                if (!(next instanceof LongAccumulator)) {
                    continue;
                }
                String metricName = next.name().get();
                String value = String.valueOf(next.value());
                prefixedValues.put(metricPrefix + metricName, value);
                values.put(metricName, value);
            }
            currentTask.appendMetrics(values);
            currentTask.appendPrefixedMetrics(prefixedValues);
        } catch (Exception e) {
            LOG.error("Unable to save spark metrics", e);
        }
    }

    public void setDriverTask(TaskRun driverTask) {
        this.driverTask = driverTask;
    }
}
