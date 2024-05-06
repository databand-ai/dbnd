/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
 */

package ai.databand;

import ai.databand.config.DbndConfig;
import ai.databand.id.Uuid5;
import ai.databand.schema.AddTaskRuns;
import ai.databand.schema.AirflowTaskContext;
import ai.databand.schema.ErrorInfo;
import ai.databand.schema.InitRun;
import ai.databand.schema.InitRunArgs;
import ai.databand.schema.LogDataset;
import ai.databand.schema.LogDatasets;
import ai.databand.schema.LogMetric;
import ai.databand.schema.LogMetrics;
import ai.databand.schema.LogTarget;
import ai.databand.schema.LogTargets;
import ai.databand.schema.Metric;
import ai.databand.schema.NewRunInfo;
import ai.databand.schema.RootRun;
import ai.databand.schema.SaveExternalLinks;
import ai.databand.schema.SaveTaskRunLog;
import ai.databand.schema.SetRunState;
import ai.databand.schema.TaskDefinition;
import ai.databand.schema.TaskRun;
import ai.databand.schema.TaskRunAttemptUpdate;
import ai.databand.schema.TaskRunEnv;
import ai.databand.schema.TaskRunsInfo;
import ai.databand.schema.TrackingSource;
import ai.databand.schema.UpdateTaskRunAttempts;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.net.ConnectException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DBND tracking API client.
 */
public class DbndClient {

    private static final DbndAppLog LOG = new DbndAppLog(LoggerFactory.getLogger(DbndClient.class));

    private final DbndApi dbnd;
    private final DbndConfig config;

    public DbndClient(DbndConfig dbndConfig) {
        config = dbndConfig;
        dbnd = new DbndApiBuilder(dbndConfig).build();
    }

    /**
     * Init new DBND run.
     *
     * @param jobName            job name
     * @param runId              rui id
     * @param user               user
     * @param taskRunsInfo       task runs info
     * @param airflowTaskContext airflow task context
     * @param root               root run definition
     * @return runUid of created run
     */
    public String initRun(String jobName,
                          String runId,
                          String user,
                          String runName,
                          TaskRunsInfo taskRunsInfo,
                          AirflowTaskContext airflowTaskContext,
                          RootRun root) {
        return this.initRun(jobName, runId, user, runName, taskRunsInfo, airflowTaskContext, root, null, null, null);
    }

    /**
     * Init new DBND run.
     *
     * @param jobName            job name
     * @param runId              rui id
     * @param user               user
     * @param taskRunsInfo       task runs info
     * @param airflowTaskContext airflow task context
     * @param root               root run definition
     * @param source             tracking source: "airflow" or "azkaban"
     * @param trackingSource     tracking source definition
     * @return runUid of created run
     */
    public String initRun(String jobName,
                          String runId,
                          String user,
                          String runName,
                          TaskRunsInfo taskRunsInfo,
                          AirflowTaskContext airflowTaskContext,
                          RootRun root,
                          String source,
                          TrackingSource trackingSource,
                          String projectName) {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        String runUid = new Uuid5("RUN_UID", runId).toString();
        String driverTaskUid = new Uuid5("DRIVER_TASK", runId).toString();
        String taskRunEnvUid = new Uuid5("TASK_RUN_ENV_UID", runId).toString();
        String userCodeVersion = new Uuid5("USER_CODE_VERSION", runId).toString();

        String machine = "";
        String databandVersion = "";

        String env = "local";
        String cloudType = "local";
        RootRun rootRun = root == null
            ? new RootRun("", null, runUid, null)
            : root;
        InitRun data = new InitRun(
            new InitRunArgs(
                runUid,
                rootRun.getRootRunUid(),
                driverTaskUid,
                new NewRunInfo(
                    null,
                    env,
                    null,
                    "jvm",
                    now,
                    now,
                    false,
                    "jvm",
                    false,
                    runUid,
                    cloudType,
                    "",
                    runUid,
                    jobName,
                    user,
                    null,
                    runName,
                    "RUNNING",
                    now,
                    rootRun,
                    projectName
                ),
                new TaskRunEnv(
                    "None",
                    taskRunEnvUid,
                    user,
                    userCodeVersion,
                    machine,
                    "",
                    now,
                    databandVersion,
                    "/",
                    true
                ),
                taskRunsInfo,
                airflowTaskContext,
                source,
                trackingSource
            )
        );

        Call<Void> call = dbnd.initRun(data);
        Optional<Object> res = safeExecuteVoid(call);
        if (res.isPresent()) {
            LOG.info("[root_run_uid: {}, job_name: {}, run_name: {}] run created", runUid, jobName, runName);
            return runUid;
        } else {
            LOG.error("[root_run_uid: {}, job_name: {}, run_name: {}] init_run HTTP request to tracker failed", runUid, jobName, runName);
        }

        throw new RuntimeException("Unable to init run because HTTP request to the tracker failed. " +
            "Check messages above for error details. " +
            "Your run will continue but it won't be tracked by Databand");
    }

    /**
     * Add task runs (method executions).
     *
     * @param rootRunUid      root run uid to add tasks to
     * @param runId           run id
     * @param taskRuns        task graph
     * @param taskDefinitions task definitions list
     * @param parentChildMap  parent-child map
     * @param upstreamsMap    upstreams map
     * @return TaskRunsInfo object
     */
    public TaskRunsInfo addTaskRuns(String rootRunUid,
                                    String runId,
                                    List<TaskRun> taskRuns,
                                    List<TaskDefinition> taskDefinitions,
                                    List<List<String>> parentChildMap,
                                    List<List<String>> upstreamsMap) {
        String taskRunEnvUid = new Uuid5("TASK_RUN_ENV_UID", runId).toString();

        TaskRunsInfo taskRunsInfo = new TaskRunsInfo(
            taskRunEnvUid,
            parentChildMap,
            rootRunUid,
            taskRuns,
            Collections.emptyList(),
            rootRunUid,
            upstreamsMap,
            true,
            taskDefinitions,
            null
        );

        Call<Void> call = dbnd.addTaskRuns(new AddTaskRuns(taskRunsInfo));
        Optional<Object> res = safeExecuteVoid(call);
        if (res.isPresent()) {
            for (TaskRun task : taskRuns) {
                LOG.info("[task_run_uid: {}, task_name: {}] task created", task.getTaskRunUid(), task.getName());
            }
        } else {
            LOG.error("[root_run_uid: {}] unable to add tasks", rootRunUid);
        }

        return taskRunsInfo;
    }

    /**
     * Update task run attempts with given state.
     *
     * @param taskRunUid        task run UID
     * @param taskRunAttemptUid task run attempt UID
     * @param state             state: RUNNING, FAILED, SUCCESS
     * @param errorInfo         error details in case of failure
     * @param startDate         task start date—required for proper task duration calculation
     */
    public void updateTaskRunAttempt(String taskRunUid,
                                     String taskRunAttemptUid,
                                     String state,
                                     ErrorInfo errorInfo,
                                     ZonedDateTime startDate) {
        updateTaskRunAttempt(taskRunUid, taskRunAttemptUid, state, errorInfo, startDate, null);
    }

    /**
     * Update task run attempts with given state.
     *
     * @param taskRunUid        task run UID
     * @param taskRunAttemptUid task run attempt UID
     * @param state             state: RUNNING, FAILED, SUCCESS
     * @param errorInfo         error details in case of failure
     * @param startDate         task start date—required for proper task duration calculation
     * @param linksDict         external links, e.g. Airflow or Azkaban run, Spark History server
     */
    public void updateTaskRunAttempt(String taskRunUid,
                                     String taskRunAttemptUid,
                                     String state,
                                     ErrorInfo errorInfo,
                                     ZonedDateTime startDate,
                                     Map<String, String> linksDict) {
        UpdateTaskRunAttempts taskRunAttempts = new UpdateTaskRunAttempts(
            Collections.singletonList(
                new TaskRunAttemptUpdate(
                    taskRunUid,
                    taskRunAttemptUid,
                    state,
                    ZonedDateTime.now(ZoneOffset.UTC),
                    startDate,
                    errorInfo,
                    linksDict
                )
            )
        );

        Call<Void> call = dbnd.updateTaskRunAttempts(taskRunAttempts);
        Optional<Object> res = safeExecuteVoid(call);
        if (res.isPresent()) {
            LOG.info("[task_run_uid: {}, task_run_attempt_uid: {}] task updated with state [{}]", taskRunUid, taskRunAttemptUid, state);
        } else {
            LOG.error("[task_run_uid: {}, task_run_attempt_uid: {}] unable to update task with state [{}]", taskRunUid, taskRunAttemptUid, state);
        }
    }

    /**
     * Set run state.
     *
     * @param runUid task run UID
     * @param state  state: RUNNING, FAILED, SUCCESS
     */
    public void setRunState(String runUid, String state) {
        SetRunState data = new SetRunState(
            runUid,
            state,
            ZonedDateTime.now(ZoneOffset.UTC)
        );

        Call<Void> call = dbnd.setRunState(data);
        Optional<Object> res = safeExecuteVoid(call);
        if (res.isPresent()) {
            LOG.info("[root_run_uid: {}] run state set to [{}]", runUid, state);
        } else {
            LOG.error("[root_run_uid: {}] unable to set run state to [{}]", runUid, state);
        }
    }

    /**
     * Log task metrics.
     *
     * @param taskRun task run
     * @param key     metric key
     * @param value   metric value
     * @param source  metric source, e.g. "user", "system", "spark"
     */
    public void logMetric(TaskRun taskRun, String key, String value, String source) {
        logMetrics(taskRun, Collections.singletonMap(key, value), source);
    }

    private final static int MAX_METRICS_TO_DISPLAY = 10;

    /**
     * Log task metrics.
     *
     * @param taskRun task run
     * @param metrics metrics map
     * @param source  metrics source, e.g. "user", "system", "spark"
     */
    public void logMetrics(TaskRun taskRun, Map<String, Object> metrics, String source) {
        if (metrics.isEmpty()) {
            return;
        }

        Set<String> metricsKeys = metrics.keySet();
        Collection<String> keysToLog = metricsKeys.size() > MAX_METRICS_TO_DISPLAY
            ? metricsKeys.stream().limit(MAX_METRICS_TO_DISPLAY).collect(Collectors.toList())
            : metricsKeys;
        LOG.info("[task_run_uid: {}, task_name: {}] logging metrics. Total: {}, Keys: {}", taskRun.getTaskRunUid(), taskRun.getName(), metricsKeys.size(), keysToLog);

        List<LogMetric> metricsInfo = metrics.entrySet().stream().map(
            m -> new LogMetric(
                taskRun.getTaskRunAttemptUid(),
                new Metric(
                    m.getKey(),
                    m.getValue(),
                    ZonedDateTime.now(ZoneOffset.UTC)
                ),
                source
            )
        ).collect(Collectors.toList());

        Optional<Object> res = safeExecuteVoid(dbnd.logMetrics(new LogMetrics(metricsInfo)));
        if (res.isPresent()) {
            LOG.info("[task_run_uid: {}, task_name: {}] metrics logged: Total: {}, Keys: {}", taskRun.getTaskRunUid(), taskRun.getName(), metricsKeys.size(), keysToLog);
        } else {
            LOG.error("[task_run_uid: {}, task_name: {}] unable to log metrics", taskRun.getTaskRunUid(), taskRun.getName());
        }
    }

    /**
     * Log task targets.
     *
     * @param taskRun task run
     * @param targets targets to log
     */
    public void logTargets(TaskRun taskRun, List<LogTarget> targets) {
        Optional<Object> res = safeExecuteVoid(dbnd.logTargets(new LogTargets(targets)));
        if (res.isPresent()) {
            LOG.info("[task_run_uid: {}, task_name: {}] targets submitted", taskRun.getTaskRunUid(), taskRun.getName());
        } else {
            LOG.error("[task_run_uid: {}, task_name: {}] unable to submit targets", taskRun.getTaskRunUid(), taskRun.getName());
        }
    }

    /**
     * Log task dataset operations.
     *
     * @param taskRun  task run
     * @param datasets dataset operations to log
     */
    public void logDatasetOperations(TaskRun taskRun, List<LogDataset> datasets) {
        for (LogDataset op : datasets) {
            LOG.info("[task_run_uid: {}, task_name: {}] logging dataset operation {}", taskRun.getTaskRunUid(), taskRun.getName(), op);
        }
        Optional<Object> res = safeExecuteVoid(dbnd.logDatasets(new LogDatasets(datasets)));
        if (res.isPresent()) {
            LOG.info("[task_run_uid: {}, task_name: {}] dataset operations submitted", taskRun.getTaskRunUid(), taskRun.getName());
        } else {
            LOG.error("[task_run_uid: {}, task_name: {}] unable to submit dataset operations", taskRun.getTaskRunUid(), taskRun.getName());
        }
    }

    /**
     * Save task run attempt external link.
     *
     * @param taskRunAttemptUid task run attempt UID
     * @param name              link name, e.g. "Azkaban execution"
     * @param url               link URL
     */
    public void saveExternalLinks(String taskRunAttemptUid, String name, String url) {
        Optional<Object> res = safeExecuteVoid(dbnd.saveExternalLinks(new SaveExternalLinks(taskRunAttemptUid, Collections.singletonMap(name, url))));
        if (res.isPresent()) {
            LOG.info("[task_run_attempt_uid: {}] external link saved", taskRunAttemptUid);
        } else {
            LOG.error("[task_run_attempt_uid: {}] Unable to save external link", taskRunAttemptUid);
        }
    }

    /**
     * Save task run attempt external link.
     *
     * @param taskRunAttemptUid task run attempt UID
     * @param linksDict         links map
     */
    public void saveExternalLinks(String taskRunAttemptUid, Map<String, String> linksDict) {
        Optional<Object> res = safeExecuteVoid(dbnd.saveExternalLinks(new SaveExternalLinks(taskRunAttemptUid, linksDict)));
        if (res.isPresent()) {
            LOG.info("[task_run_attempt_uid: {}] external link saved", taskRunAttemptUid);
        } else {
            LOG.error("[task_run_attempt_uid: {}] Unable to save external link", taskRunAttemptUid);
        }
    }

    /**
     * Save task logs.
     *
     * @param taskRunAttemptUid task run attempt UID
     * @param logBody           log body
     */
    public void saveTaskLog(String taskRunUid, String taskRunAttemptUid, String logBody) {
        if (logBody == null) {
            return;
        }
        LOG.info("[task_run_uid: {}, task_run_attempt_uid: {}] submitting task log, log size: {} characters", taskRunUid, taskRunAttemptUid, logBody.length());
        SaveTaskRunLog body = new SaveTaskRunLog(config, taskRunAttemptUid, logBody);
        Optional<Object> res = safeExecuteVoid(dbnd.saveTaskRunLog(body));
        if (res.isPresent()) {
            LOG.info("[task_run_uid: {}, task_run_attempt_uid: {}] task log submitted", taskRunUid, taskRunAttemptUid);
        } else {
            LOG.error("[task_run_uid: {}, task_run_attempt_uid: {}] Unable to submit task log", taskRunUid, taskRunAttemptUid);
        }
    }

    /**
     * Wrap retrofit exception and response handling for Void calls.
     *
     * @param call        prepared Retrofit HTTP call
     * @param logToStdout set to true if logging to stdout is required, e.g. when log system isn't initialized
     * @return execute result wrapped in Optional
     */
    protected Optional<Object> safeExecuteVoid(Call<Void> call) {
        try {
            if(config.isVerbose()) {
                LOG.info("Executing HTTP request to the Databand tracker, URL of the request: %s", call.request().url());
            }

            Response<Void> res = call.execute();
            if (res.isSuccessful()) {
                return Optional.of(new Object());
            } else {
                String errorMsg = String.format("HTTP request to the Databand tracker '%s' failed: %s %s", config.databandUrl(), res.code(), res.message());
                LOG.error(errorMsg);
                if(call.request().url().encodedPath().contains("tracking/init_run")) {
                    DbndAppLog.printfln(Level.ERROR, errorMsg); // aditional printing for a first http call
                }
                if (res.code() == 401) {
                    LOG.warn("Check DBND__CORE__DATABAND_ACCESS_TOKEN variable. Looks like token is missing or wrong");
                } else if (res.code() >= 500) {
                    LOG.warn("Check if Databand server is running at %s", config.databandUrl());
                } else {
                    LOG.warn("Make sure Databand tracker is up and running at the %s", config.databandUrl());
                }
                return Optional.empty();
            }
        } catch (ConnectException ex) {
            LOG.error("Could not connect to the tracking server at %s. " +
                "Check that server is available and Databand tracker is up and running.\n" +
                "Exception: %s", config.databandUrl(), ex.getMessage());
            return Optional.empty();
        } catch (IOException e) {
            LOG.error("HTTP request to the tracking server at %s failed.\nException: %s", config.databandUrl(), e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * API client.
     *
     * @return API client.
     */
    public DbndApi api() {
        return dbnd;
    }
}
