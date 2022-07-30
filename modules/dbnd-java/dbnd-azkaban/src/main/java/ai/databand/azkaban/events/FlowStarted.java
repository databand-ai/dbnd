/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban.events;

import ai.databand.DbndClient;
import ai.databand.azkaban.AzkabanEvent;
import ai.databand.azkaban.AzkabanFlow;
import ai.databand.config.DbndConfig;
import ai.databand.schema.AzkabanTaskContext;
import ai.databand.schema.TrackingSource;

public class FlowStarted implements AzkabanEvent {

    private final DbndClient dbnd;
    private final AzkabanTaskContext azCtx;
    private final AzkabanFlow flow;

    public FlowStarted(DbndConfig config, AzkabanTaskContext azCtx, AzkabanFlow flow) {
        this.dbnd = new DbndClient(config);
        this.azCtx = azCtx;
        this.flow = flow;
    }

    @Override
    public void track() {
        if (!flow.isTrack()) {
            return;
        }
        dbnd.initRun(
            flow.pipelineName(),
            flow.uuid(),
            flow.user(),
            azCtx.runName(),
            flow.toDataband(),
            null,
            null,
            null,
            new TrackingSource(azCtx),
            azCtx.projectName()
        );
    }
}
