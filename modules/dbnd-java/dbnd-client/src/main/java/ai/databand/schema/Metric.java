/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import ai.databand.schema.jackson.ZonedDateTimeDeserializer;
import ai.databand.schema.jackson.ZonedDateTimeSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.ZonedDateTime;

public class Metric {

    private final String key;
    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private final ZonedDateTime timestamp;
    private Object value;
    private Integer valueInt;
    private Double valueFloat;
    private Object valueJson;
    private String valueStr;

    public Metric(String key, Object metricValue, ZonedDateTime timestamp) {
        this.key = key;
        this.value = metricValue;

        if (metricValue instanceof String) {
            this.valueStr = (String) metricValue;
            try {
                valueInt = new Integer(valueStr);
            } catch (NumberFormatException e) {
                valueInt = null;
            }

            if (valueStr.contains(".")) {
                try {
                    valueFloat = new Double(valueStr);
                } catch (NumberFormatException e) {
                    valueFloat = null;
                }
            }
        } else if (metricValue instanceof Long) {
            Long value = (Long) metricValue;
            if (value <= Integer.MAX_VALUE) {
                this.valueInt = value.intValue();
            } else {
                this.valueStr = value.toString();
            }
        } else if (metricValue instanceof Integer) {
            this.valueInt = (Integer) metricValue;
        } else if (metricValue instanceof Double) {
            this.valueFloat = (Double) metricValue;
        } else {
            this.valueJson = metricValue;
        }

        this.timestamp = timestamp;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public Integer getValueInt() {
        return valueInt;
    }

    public Double getValueFloat() {
        return valueFloat;
    }

    public Object getValueJson() {
        return valueJson;
    }

    public String getValueStr() {
        return valueStr;
    }
}
