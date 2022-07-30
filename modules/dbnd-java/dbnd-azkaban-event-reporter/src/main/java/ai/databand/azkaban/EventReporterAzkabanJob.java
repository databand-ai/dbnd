/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban;

import ai.databand.config.DbndConfig;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.utils.FileIOUtils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

public class EventReporterAzkabanJob extends AzkabanJob {

    private final String jobId;
    private final String executionId;
    private final DbndConfig config;
    private final boolean isFailed;
    private final String startTime;

    public EventReporterAzkabanJob(DbndConfig config,
                                   String jobId,
                                   String executionId,
                                   boolean isFailed,
                                   String startTime) {
        this.jobId = jobId;
        this.executionId = executionId;
        this.config = config;
        this.isFailed = isFailed;
        this.startTime = startTime;
    }

    @Override
    public String state() {
        return isFailed ? "FAILED" : "SUCCESS";
    }

    @Override
    public String log() {
        ExecutorManagerAdapter executorManagerAdapter = SERVICE_PROVIDER.getInstance(ExecutorManagerAdapter.class);

        try {
            ExecutableFlow exFlow = executorManagerAdapter.getExecutableFlow(Integer.parseInt(executionId));
            ExecutableNode node = exFlow.getExecutableNodePath(jobId);
            final int attempt = node.getAttempt();
            final FileIOUtils.LogData data = executorManagerAdapter.getExecutionJobLog(exFlow, jobId, 0, config.previewTotalBytes(), attempt);
            return data.getData();
        } catch (ExecutorManagerException e) {
            return "";
        }
    }

    @Override
    public boolean isFailed() {
        return isFailed;
    }

    @Override
    public ZonedDateTime startDate() {
        return Instant.ofEpochMilli(Long.parseLong(startTime)).atZone(ZoneOffset.UTC);
    }
}
