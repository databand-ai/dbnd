package ai.databand.azkaban;

import ai.databand.config.DbndConfig;
import ai.databand.log.TruncatedLog;
import azkaban.execapp.JobRunner;
import azkaban.executor.Status;

import java.io.File;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class AgentAzkabanJob extends AzkabanJob {

    private final DbndConfig config;
    private final JobRunner jobRunner;
    private final boolean isFailed;
    private final ZonedDateTime startDate;

    public AgentAzkabanJob(DbndConfig config,
                           JobRunner jobRunner,
                           String startTime) {
        this.config = config;
        this.jobRunner = jobRunner;
        this.isFailed = Status.SUCCEEDED != jobRunner.getNode().getStatus();
        this.startDate = Instant.ofEpochMilli(Long.parseLong(startTime)).atZone(ZoneOffset.UTC);
    }

    @Override
    public String state() {
        Status status = jobRunner.getNode().getStatus();
        if (Status.KILLED == status || Status.CANCELLED == status) {
            return "CANCELLED";
        }
        return Status.SUCCEEDED == status ? "SUCCESS" : "FAILED";
    }

    @Override
    public String log() {
        File logFile = jobRunner.getLogFile();
        return new TruncatedLog(config, logFile).toString();
    }

    @Override
    public boolean isFailed() {
        return isFailed;
    }

    @Override
    public ZonedDateTime startDate() {
        return startDate;
    }
}
