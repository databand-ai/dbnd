/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban.events;

import ai.databand.azkaban.AgentAzkabanFlow;
import ai.databand.azkaban.AzkabanEvent;
import ai.databand.azkaban.AzkabanFlow;
import ai.databand.azkaban.AzkabanFlowSharedProps;
import ai.databand.azkaban.AzkabanProps;
import ai.databand.config.DbndConfig;
import ai.databand.config.Env;
import ai.databand.config.JavaOpts;
import azkaban.event.Event;
import azkaban.execapp.FlowRunner;

public class FlowStartedEvent implements AzkabanEvent {

    private final AzkabanEvent origin;

    public FlowStartedEvent(Event event) {
        DbndConfig config = new DbndConfig(
            new Env(
                new JavaOpts(
                    new AzkabanProps(
                        new AzkabanFlowSharedProps((FlowRunner) event.getRunner())
                    )
                )
            )
        );

        FlowRunnerContext flowCtx = new FlowRunnerContext(event, config);
        AzkabanFlow flow = new AgentAzkabanFlow(config, flowCtx);

        this.origin = new FlowStarted(config, flowCtx.taskContext(), flow);
    }

    @Override
    public void track() {
        origin.track();
    }
}
