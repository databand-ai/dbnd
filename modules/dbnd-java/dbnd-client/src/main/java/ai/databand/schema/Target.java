/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import ai.databand.schema.jackson.ZonedDateTimeDeserializer;
import ai.databand.schema.jackson.ZonedDateTimeSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.ZonedDateTime;

public class Target {

    private String taskRunUid;
    private String parameterName;
    private String path;

    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private ZonedDateTime createdDate;

}
