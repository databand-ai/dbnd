/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import ai.databand.schema.jackson.ZonedDateTimeDeserializer;
import ai.databand.schema.jackson.ZonedDateTimeSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;

public class TaskRunAttemptUpdate {

    private final String taskRunUid;
    private final String taskRunAttemptUid;
    private final String state;

    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private final ZonedDateTime timestamp;

    private final ErrorInfo error;

    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private final ZonedDateTime startDate;

    private final Map<String, String> externalLinksDict;

    public TaskRunAttemptUpdate(String taskRunUid,
                                String taskRunAttemptUid,
                                String state,
                                ZonedDateTime timestamp,
                                ZonedDateTime startDate,
                                ErrorInfo error) {
        this(taskRunUid, taskRunAttemptUid, state, timestamp, startDate, error, Collections.emptyMap());
    }

    public TaskRunAttemptUpdate(String taskRunUid,
                                String taskRunAttemptUid,
                                String state,
                                ZonedDateTime timestamp,
                                ZonedDateTime startDate,
                                ErrorInfo error,
                                Map<String, String> externalLinksDict) {
        this.taskRunUid = taskRunUid;
        this.taskRunAttemptUid = taskRunAttemptUid;
        this.state = state;
        this.timestamp = timestamp;
        this.startDate = startDate;
        this.error = error;
        this.externalLinksDict = externalLinksDict;
    }

    public String getTaskRunUid() {
        return taskRunUid;
    }

    public String getTaskRunAttemptUid() {
        return taskRunAttemptUid;
    }

    public String getState() {
        return state;
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public ErrorInfo getError() {
        return error;
    }

    public ZonedDateTime getStartDate() {
        return startDate;
    }

    public Map<String, String> getExternalLinksDict() {
        return externalLinksDict;
    }
}
