/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import ai.databand.schema.jackson.ZonedDateTimeDeserializer;
import ai.databand.schema.jackson.ZonedDateTimeSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.ZonedDateTime;

public class SetRunState {

    private final String runUid;

    private final String state;

    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private final ZonedDateTime timeStamp;

    public SetRunState(String runUid, String state, ZonedDateTime timeStamp) {
        this.runUid = runUid;
        this.state = state;
        this.timeStamp = timeStamp;
    }

    public String getRunUid() {
        return runUid;
    }

    public String getState() {
        return state;
    }

    public ZonedDateTime getTimeStamp() {
        return timeStamp;
    }

}
