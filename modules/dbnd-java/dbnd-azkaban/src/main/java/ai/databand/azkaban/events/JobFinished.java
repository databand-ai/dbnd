package ai.databand.azkaban.events;

import ai.databand.DbndClient;
import ai.databand.azkaban.AzkabanDbndConfig;
import ai.databand.azkaban.AzkabanEvent;
import ai.databand.azkaban.AzkabanJob;
import ai.databand.config.DbndConfig;
import ai.databand.schema.AzkabanTaskContext;
import ai.databand.schema.ErrorInfo;

import java.util.Optional;

public class JobFinished implements AzkabanEvent {

    private final DbndConfig config;
    private final AzkabanJob job;
    private final AzkabanTaskContext azCtx;
    private final DbndClient dbnd;
    private final String failureMessage;

    public JobFinished(DbndConfig config, AzkabanTaskContext azCtx, AzkabanJob job, String failureMessage) {
        this.config = config;
        this.dbnd = new DbndClient(config);
        this.azCtx = azCtx;
        this.job = job;
        this.failureMessage = failureMessage;
    }

    @Override
    public void track() {
        AzkabanDbndConfig azConfig = new AzkabanDbndConfig(config);
        if (!azConfig.isTrackingEnabled(azCtx)) {
            return;
        }
        dbnd.saveTaskLog(azCtx.taskRunUid(), azCtx.taskRunAttemptUid(), job.log());
        Optional<ErrorInfo> errorInfo = Optional.empty();
        if (job.isFailed()) {
            // todo: find failure description
            errorInfo = Optional.of(new ErrorInfo(
                failureMessage,
                "",
                false,
                "",
                "",
                "",
                false,
                "AzkabanExecutionError"
            ));

        }
        dbnd.updateTaskRunAttempt(
            azCtx.taskRunUid(),
            azCtx.taskRunAttemptUid(),
            job.state(),
            errorInfo.orElse(null),
            job.startDate()
        );
    }


}
