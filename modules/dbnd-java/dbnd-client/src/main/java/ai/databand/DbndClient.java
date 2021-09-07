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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger LOG = LoggerFactory.getLogger(DbndClient.class);

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
        Optional<Object> res = safeExecuteVoid(call, true);
        if (res.isPresent()) {
            LOG.info("[task_run: {}] Run created", runId);
            return runUid;
        } else {
            LOG.error("[task_run: {}] Unable to create run", runId);
        }

        throw new RuntimeException("Unable to create run");
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
            LOG.info("[task_run: {}] Task created", runId);
        } else {
            LOG.error("[task_run: {}] Unable to add task", runId);
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
            LOG.info("[task_run: {}] [task_run_attempt: {}] Updated with status {}", taskRunUid, taskRunAttemptUid, state);
        } else {
            LOG.error("[task_run: {}] Unable to complete task run attempt", taskRunUid);
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
            LOG.info("[task_run: {}] Completed", runUid);
        } else {
            LOG.error("[task_run: {}] Unable to complete run", runUid);
        }
    }

    /**
     * Log task metrics.
     *
     * @param taskRunUid task run UID
     * @param key        metric key
     * @param value      metric value
     * @param source     metric source, e.g. "user", "system", "spark"
     */
    public void logMetric(String taskRunUid, String key, String value, String source) {
        logMetrics(taskRunUid, Collections.singletonMap(key, value), source);
    }

    private final static int MAX_METRICS_TO_DISPLAY = 10;

    /**
     * Log task metrics.
     *
     * @param taskRunUid task run UID
     * @param metrics    metrics map
     * @param source     metrics source, e.g. "user", "system", "spark"
     */
    public void logMetrics(String taskRunUid, Map<String, Object> metrics, String source) {
        if (metrics.isEmpty()) {
            return;
        }

        Set<String> metricsKeys = metrics.keySet();
        Collection<String> keysToLog = metricsKeys.size() > MAX_METRICS_TO_DISPLAY
            ? metricsKeys.stream().limit(MAX_METRICS_TO_DISPLAY).collect(Collectors.toList())
            : metricsKeys;
        LOG.info("[task_run: {}] Logging metrics. Total: {}, Keys: {}", taskRunUid, metricsKeys.size(), keysToLog);

        List<LogMetric> metricsInfo = metrics.entrySet().stream().map(
            m -> new LogMetric(
                taskRunUid,
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
            LOG.info("[task_run: {}] Metrics logged: Total: {}, Keys: {}", taskRunUid, metricsKeys.size(), keysToLog);
        } else {
            LOG.error("[task_run: {}] Unable to log metrics", taskRunUid);
        }
    }

    /**
     * Log task targets.
     *
     * @param taskRunUid task run UID
     * @param targets    targets to log
     */
    public void logTargets(String taskRunUid, List<LogTarget> targets) {
        Optional<Object> res = safeExecuteVoid(dbnd.logTargets(new LogTargets(targets)));
        if (res.isPresent()) {
            LOG.info("[task_run: {}] target submitted", taskRunUid);
        } else {
            LOG.error("[task_run: {}] Unable to submit target", taskRunUid);
        }
    }

    /**
     * Log task dataset operations.
     *
     * @param taskRunUid task run UID
     * @param datasets   dataset operations to log
     */
    public void logDatasetOperations(String taskRunUid, List<LogDataset> datasets) {
        Optional<Object> res = safeExecuteVoid(dbnd.logDatasets(new LogDatasets(datasets)));
        if (res.isPresent()) {
            LOG.info("[task_run: {}] dataset operation submitted", taskRunUid);
        } else {
            LOG.error("[task_run: {}] Unable to submit dataset operation", taskRunUid);
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
            LOG.info("[task_run: {}] external link saved", taskRunAttemptUid);
        } else {
            LOG.error("[task_run: {}] Unable to save external link", taskRunAttemptUid);
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
            LOG.info("[task_run: {}] external link saved", taskRunAttemptUid);
        } else {
            LOG.error("[task_run: {}] Unable to save external link", taskRunAttemptUid);
        }
    }

    /**
     * Save task logs.
     *
     * @param taskRunAttemptUid task run attempt UID
     * @param logBody           log body
     */
    public void saveTaskLog(String taskRunAttemptUid, String logBody) {
        LOG.info("[task_run: {}] saving task log, log size: {} characters", taskRunAttemptUid, logBody.length());
        SaveTaskRunLog body = new SaveTaskRunLog(config, taskRunAttemptUid, logBody);
        Optional<Object> res = safeExecuteVoid(dbnd.saveTaskRunLog(body));
        if (res.isPresent()) {
            LOG.info("[task_run: {}] task log submitted", taskRunAttemptUid);
        } else {
            LOG.error("[task_run: {}] Unable to submit task log", taskRunAttemptUid);
        }
    }

    /**
     * Wrap retrofit exception and response handling for Void calls.
     *
     * @param call prepared Retrofit HTTP call
     * @return execute result wrapped in Optional
     */
    protected Optional<Object> safeExecuteVoid(Call<Void> call) {
        return safeExecuteVoid(call, false);
    }

    /**
     * Wrap retrofit exception and response handling for Void calls.
     *
     * @param call        prepared Retrofit HTTP call
     * @param logToStdout set to true if logging to stdout is required, e.g. when log system isn't initialized
     * @return execute result wrapped in Optional
     */
    protected Optional<Object> safeExecuteVoid(Call<Void> call, boolean logToStdout) {
        try {
            Response<Void> res = call.execute();
            if (res.isSuccessful()) {
                return Optional.of(new Object());
            } else {
                return Optional.empty();
            }
        } catch (ConnectException ex) {
            String msg = String.format("Could not connect to server: %s, %s", call.request().url(), ex.getMessage());
            if (logToStdout) {
                System.out.println(msg);
            } else {
                LOG.error(msg);
            }
            return Optional.empty();
        } catch (IOException e) {
            String msg = String.format("Unable to perform HTTP request to server: %s, %s", call.request().url(), e.getMessage());
            if (logToStdout) {
                System.out.println(msg);
            } else {
                LOG.error(msg);
            }
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
