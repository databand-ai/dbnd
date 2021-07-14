package ai.databand.mlflow;

import ai.databand.DbndApi;
import ai.databand.DbndApiBuilder;
import ai.databand.RandomNames;
import ai.databand.config.DbndConfig;
import ai.databand.id.Sha1Long;
import ai.databand.id.Sha1Short;
import ai.databand.id.Uuid5;
import ai.databand.schema.InitRunArgs;
import ai.databand.schema.InitRun;
import ai.databand.schema.LogMetric;
import ai.databand.schema.Metric;
import ai.databand.schema.NewRunInfo;
import ai.databand.schema.RootRun;
import ai.databand.schema.SetRunState;
import ai.databand.schema.TaskDefinition;
import ai.databand.schema.TaskParamDefinition;
import ai.databand.schema.TaskRun;
import ai.databand.schema.TaskRunAttemptUpdate;
import ai.databand.schema.TaskRunEnv;
import ai.databand.schema.TaskRunParam;
import ai.databand.schema.TaskRunsInfo;
import ai.databand.schema.UpdateTaskRunAttempts;
import org.mlflow.api.proto.Service;
import org.mlflow.tracking.MlflowClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Response;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;

public class DbndMlflowClient extends MlflowClient {

    private static final Logger LOG = LoggerFactory.getLogger(DbndMlflowClient.class);

    private final DbndApi dbnd;
    private final MlflowClient mlflowClient;
    private final String dbndTrackingUri;

    public static MlflowClient newClient() {
        DbndConfig config = new DbndConfig();
        String trackerUrl = config.databandUrl();
        MlflowClient mlflowClient = System.getenv("MLFLOW_TRACKING_URI") == null ? new StubMlFlowClient() : new MlflowClient();

        return new DbndMlflowClient(mlflowClient, trackerUrl);
    }

    public DbndMlflowClient(MlflowClient mlflowClient, String dbndTrackingUri) {
        this(mlflowClient, dbndTrackingUri, new DbndConfig());
    }

    public DbndMlflowClient(MlflowClient mlflowClient, String dbndTrackingUri, DbndConfig config) {
        super("http://127.0.0.1");
        this.dbndTrackingUri = dbndTrackingUri;
        this.mlflowClient = mlflowClient;

        dbnd = new DbndApiBuilder(config).build();
    }

    public DbndApi dbndApi() {
        return dbnd;
    }

    @Override
    public Service.RunInfo createRun(Service.CreateRun request) {
        Service.RunInfo mlFlowRunInfo = mlflowClient.createRun(request);

        String experimentId = request.getExperimentId();
        String userId = request.getUserId();

        String runUid = uuid5("RUN_UID", experimentId);

        String rootRunUid = runUid;

        String driverTaskUid = uuid5("DRIVER_TASK", experimentId);

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        String jobName = "embed_mlflow";

        String rootRunUrl = String.format("%s/app/jobs/%s/%s", dbndTrackingUri, jobName, runUid);

        String taskRunEnvUid = uuid5("TASK_RUN_ENV_UID", experimentId);

        String taskRunUid = driverTaskUid;

        String userCodeVersion = uuid5("USER_CODE_VERSION", experimentId);

        String taskRunAttemptUid = uuid5("TASK_RUN_ATTEMPT", experimentId);

        String machine = "mlflow machine";

        String cmd = "mlflow run";

        String databandVersion = "0.24.0";

        String version = "mlflowVersion";

        String taskSignature = sha1short("TASK_SIGNATURE", experimentId);

        String taskDefinitionUid = uuid5("TASK_DEFINITION", experimentId);

        String taskAfId = "mlflow_driver__" + sha1short("TASK_AF", experimentId);

        String env = "mlflow";
        InitRun data = new InitRun(
            new InitRunArgs(
                runUid,
                rootRunUid,
                driverTaskUid,
                new NewRunInfo(
                    null,
                    env,
                    null,
                    "mlflow_inproces",
                    now,
                    now,
                    false,
                    "mlflow",
                    false,
                    runUid,
                    "mlflow cloud",
                    "unknown",
                    runUid,
                    jobName,
                    userId,
                    null,
                    RandomNames.next(),
                    "RUNNING",
                    now,
                    new RootRun(
                        rootRunUrl,
                        null,
                        rootRunUid,
                        null
                    )
                ),
                new TaskRunEnv(
                    "None",
                    taskRunEnvUid,
                    userId,
                    userCodeVersion,
                    machine,
                    cmd,
                    now,
                    databandVersion,
                    "/",
                    true
                ),
                new TaskRunsInfo(
                    taskRunEnvUid,
                    Collections.emptyList(),
                    runUid,
                    Collections.singletonList(
                        new TaskRun(
                            runUid,
                            true,
                            false,
                            null,
                            version,
                            taskRunUid,
                            taskSignature,
                            jobName,
                            Collections.singletonList(
                                new TaskRunParam(
                                    "True",
                                    "context",
                                    "_meta_input"
                                )
                            ),
                            taskSignature,
                            false,
                            now.toLocalDate(),
                            now,
                            "/logs",
                            "RUNNING",
                            taskDefinitionUid,
                            cmd,
                            false,
                            false,
                            taskRunAttemptUid,
                            taskAfId,
                            false,
                            false,
                            cmd,
                            taskAfId,
                            "mlflow",
                            Collections.emptyMap()
                        )
                    ),
                    Collections.emptyList(),
                    rootRunUid,
                    Collections.emptyList(),
                    false,
                    Collections.singletonList(
                        new TaskDefinition(
                            "mlflow_task",
                            "...",
                            sha1long("SOURCE", experimentId),
                            "",
                            taskDefinitionUid,
                            sha1long("MODULE_SOURCE", experimentId),
                            Collections.singletonList(
                                new TaskParamDefinition(
                                    "_meta_input",
                                    "task_input",
                                    "user",
                                    false,
                                    true,
                                    "bool",
                                    "",
                                    "NOTHING"
                                )
                            ),
                            "mlflow_task",
                            "java",
                            "..."
                        )
                    ),
                    null
                )
            )
        );

        try {
            Response<Void> res = dbnd.initRun(data).execute();
            if (res.isSuccessful()) {
                LOG.info("[task_run: {}] Run created", experimentId);
            } else {
                LOG.error("[task_run: {}] Unable to create run", experimentId);
            }
        } catch (IOException e) {
            LOG.error(String.format("[task_run: %s] Unable to create run", data.getInitArgs()), e);
        }

        return mlFlowRunInfo;
    }

    /**
     * We should complete task run attempt as well as task run here.
     *
     * @param runId MlFlow run id
     */
    @Override
    public void setTerminated(String runId) {
        mlflowClient.setTerminated(runId);

        String runUid = uuid5("RUN_UID", runId);
        String taskRunAttemptUid = uuid5("TASK_RUN_ATTEMPT", runId);

        UpdateTaskRunAttempts taskRunAttempts = new UpdateTaskRunAttempts(
            Collections.singletonList(
                new TaskRunAttemptUpdate(
                    runUid,
                    taskRunAttemptUid,
                    "SUCCESS",
                    ZonedDateTime.now(ZoneOffset.UTC),
                    ZonedDateTime.now(ZoneOffset.UTC),
                    null
                )
            )
        );

        try {
            Response<Void> res = dbnd.updateTaskRunAttempts(taskRunAttempts).execute();
            if (res.isSuccessful()) {
                LOG.info("[task_run: {}] Completed", runUid);
            } else {
                LOG.error("[task_run: {}] Unable to complete task run attempt", runUid);
            }
        } catch (IOException e) {
            LOG.error(String.format("[task_run: %s] Unable to complete task run attempt", runUid), e);
        }

        SetRunState data = new SetRunState(
            runUid,
            "SUCCESS",
            ZonedDateTime.now(ZoneOffset.UTC)
        );
        try {
            Response<Void> res = dbnd.setRunState(data).execute();
            if (res.isSuccessful()) {
                LOG.info("[task_run: {}] Completed", runUid);
            } else {
                LOG.error("[task_run: {}] Unable to complete run", runUid);
            }
        } catch (IOException e) {
            LOG.error(String.format("[task_run: %s] Unable to complete run", runUid), e);
        }
    }

    @Override
    public void logMetric(String runId, String key, double value, long timestamp, long step) {
        String taskRunAttemptUid = uuid5("TASK_RUN_ATTEMPT", runId);
        mlflowClient.logMetric(runId, key, value, timestamp, step);
        LogMetric data = new LogMetric(
            taskRunAttemptUid,
            new Metric(
                key,
                String.valueOf(value),
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC)
            )
        );
        try {
            Response<Void> res = dbnd.logMetric(data).execute();
            if (res.isSuccessful()) {
                LOG.info("[task_run: {}] Sent metric [{}]:[{}]", taskRunAttemptUid, key, value);
            } else {
                LOG.error("[task_run: {}] Unable to send metric", taskRunAttemptUid);
            }
        } catch (IOException e) {
            LOG.error(String.format("[task_run: %s] Unable to send metric", taskRunAttemptUid), e);
        }
    }

    protected String sha1short(String namespace, String name) {
        return new Sha1Short(namespace, name).toString();
    }

    protected String sha1long(String namespace, String name) {
        return new Sha1Long(namespace, name).toString();
    }

    protected String uuid5(String namespace, String name) {
        return new Uuid5(namespace, name).toString();
    }

    private static class StubMlFlowClient extends MlflowClient {

        public StubMlFlowClient() {
            super("http://127.0.0.1");
        }

        @Override
        public void logMetric(String runId, String key, double value, long timestamp, long step) {
            // do nothing
        }

        @Override
        public Service.RunInfo createRun(Service.CreateRun request) {
            Service.RunInfo.Builder builder = Service.RunInfo.newBuilder();
            builder.setExperimentId(request.getExperimentId());
            builder.setStartTime(request.getStartTime());
            builder.setUserId(request.getUserId());
            return builder.build();
        }

        @Override
        public void setTerminated(String runId) {
            // do nothing
        }
    }
}
