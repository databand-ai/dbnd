/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban.events;

import ai.databand.azkaban.AgentAzkabanJob;
import ai.databand.azkaban.AzkabanEvent;
import ai.databand.azkaban.AzkabanJob;
import ai.databand.azkaban.AzkabanProps;
import ai.databand.config.DbndConfig;
import ai.databand.config.Env;
import ai.databand.config.JavaOpts;
import ai.databand.schema.AzkabanTaskContext;
import azkaban.event.Event;
import azkaban.execapp.JobRunner;
import azkaban.executor.ExecutableFlow;
import azkaban.flow.CommonJobProperties;

public class JobFinishedEvent implements AzkabanEvent {

    private final AzkabanEvent origin;

    public JobFinishedEvent(Event event) {
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
        String startTime = String.valueOf(event.getTime());

        AzkabanTaskContext azCtx = new AzkabanTaskContext(projectName, flowName, flowUuid, executionId, jobRunner.getNode().getId(), config);

        AzkabanJob job = new AgentAzkabanJob(
            config,
            jobRunner,
            startTime
        );

        this.origin = new JobFinished(config, azCtx, job, "Job failed");
    }

    @Override
    public void track() {
        origin.track();
    }
}
