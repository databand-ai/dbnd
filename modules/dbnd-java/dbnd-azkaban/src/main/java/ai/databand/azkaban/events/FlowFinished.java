/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban.events;

import ai.databand.DbndClient;
import ai.databand.azkaban.AzkabanEvent;
import ai.databand.azkaban.AzkabanFlow;
import ai.databand.config.DbndConfig;
import ai.databand.schema.AzkabanTaskContext;

public class FlowFinished implements AzkabanEvent {

    private final DbndClient dbnd;
    private final AzkabanTaskContext azCtx;
    private final AzkabanFlow flow;

    public FlowFinished(DbndConfig config, AzkabanTaskContext azCtx, AzkabanFlow flow) {
        this.dbnd = new DbndClient(config);
        this.azCtx = azCtx;
        this.flow = flow;
    }

    @Override
    public void track() {
        if (!flow.isTrack()) {
            return;
        }
        dbnd.saveTaskLog(azCtx.driverTaskUid(), azCtx.driverTaskRunAttemptUid(), flow.log());
        dbnd.updateTaskRunAttempt(azCtx.driverTaskUid(), azCtx.driverTaskRunAttemptUid(), flow.state(), null, flow.startDate());
        dbnd.setRunState(azCtx.rootRunUid(), flow.state());
    }

}
