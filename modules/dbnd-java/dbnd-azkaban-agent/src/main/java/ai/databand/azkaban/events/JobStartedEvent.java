/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban.events;

import ai.databand.azkaban.AzkabanEvent;
import ai.databand.azkaban.AzkabanProps;
import ai.databand.config.DbndConfig;
import ai.databand.config.Env;
import ai.databand.config.JavaOpts;
import ai.databand.schema.AzkabanTaskContext;
import azkaban.event.Event;
import azkaban.execapp.JobRunner;
import azkaban.executor.ExecutableFlow;
import azkaban.flow.CommonJobProperties;

public class JobStartedEvent implements AzkabanEvent {

    private final AzkabanEvent origin;

    public JobStartedEvent(Event event) {
        if (!(event.getRunner() instanceof JobRunner)) {
            origin = new EmptyEvent();
            return;
        }
        DbndConfig config = new DbndConfig(
            new Env(
                new JavaOpts(
                    new AzkabanProps()
                )
            )
        );

        JobRunner jobRunner = (JobRunner) event.getRunner();
        ExecutableFlow executableFlow = jobRunner.getNode().getExecutableFlow();
        String flowName = executableFlow.getId();
        String projectName = executableFlow.getProjectName();

        String flowUuid = executableFlow.getInputProps().get(CommonJobProperties.FLOW_UUID);
        String executionId = String.valueOf(executableFlow.getExecutionId());

        AzkabanTaskContext azCtx = new AzkabanTaskContext(projectName, flowName, flowUuid, executionId, jobRunner.getNode().getId(), config);

        this.origin = new JobStarted(config, azCtx);
    }

    @Override
    public void track() {
        origin.track();
    }
}
