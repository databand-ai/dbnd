package ai.databand.azkaban.events;

import ai.databand.DbndClient;
import ai.databand.azkaban.AzkabanDbndConfig;
import ai.databand.azkaban.AzkabanEvent;
import ai.databand.config.DbndConfig;
import ai.databand.schema.AzkabanTaskContext;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class JobStarted implements AzkabanEvent {

    private final DbndConfig config;
    private final AzkabanTaskContext azCtx;
    private final DbndClient dbnd;

    public JobStarted(DbndConfig config, AzkabanTaskContext azCtx) {
        this.config = config;
        this.dbnd = new DbndClient(config);
        this.azCtx = azCtx;
    }

    @Override
    public void track() {
        AzkabanDbndConfig azConfig = new AzkabanDbndConfig(config);
        if (!azConfig.isTrackingEnabled(azCtx)) {
            return;
        }
        dbnd.updateTaskRunAttempt(
            azCtx.taskRunUid(),
            azCtx.taskRunAttemptUid(),
            "RUNNING",
            null,
            ZonedDateTime.now(ZoneOffset.UTC)
        );
    }

}
