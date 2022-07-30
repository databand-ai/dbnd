/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskRunParam {

    private String name;
    private String value;
    private String valueOrigin;
    private String parameterName;
    private List<String> targetsUids;

    public TaskRunParam() {
    }

    public TaskRunParam(String value, String valueOrigin, String parameterName) {
        this.value = value;
        this.valueOrigin = valueOrigin;
        this.parameterName = parameterName;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValueOrigin() {
        return valueOrigin;
    }

    public void setValueOrigin(String valueOrigin) {
        this.valueOrigin = valueOrigin;
    }

    public String getParameterName() {
        return parameterName;
    }

    public void setParameterName(String parameterName) {
        this.parameterName = parameterName;
    }

    public List<String> getTargetsUids() {
        return targetsUids;
    }

    public void setTargetsUids(List<String> targetsUids) {
        this.targetsUids = targetsUids;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String toString() {
        if (name != null && value != null) {
            return String.format("[[%s]:[%s]]", name, value);
        }
        return super.toString();
    }
}
