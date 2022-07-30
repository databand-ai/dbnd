/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import ai.databand.config.DbndConfig;
import ai.databand.log.TruncatedLog;

public class SaveTaskRunLog {

    private final String taskRunAttemptUid;

    private final TruncatedLog logBody;

    public SaveTaskRunLog(DbndConfig config, String taskRunAttemptUid, String logBody) {
        this(taskRunAttemptUid, new TruncatedLog(config, logBody));
    }

    public SaveTaskRunLog(String taskRunAttemptUid, TruncatedLog logBody) {
        this.taskRunAttemptUid = taskRunAttemptUid;
        this.logBody = logBody;
    }

    public String getTaskRunAttemptUid() {
        return taskRunAttemptUid;
    }

    public String getLogBody() {
        return logBody.toString();
    }
}
