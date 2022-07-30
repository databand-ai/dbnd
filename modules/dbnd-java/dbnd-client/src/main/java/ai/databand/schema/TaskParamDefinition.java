/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskParamDefinition {

    private String name;

    private String kind;

    private String group;

    private boolean significant;

    private boolean loadOnBuild;

    private String valueType;

    private String description;

    @JsonProperty("default")
    private String defaultValue;

    public TaskParamDefinition(String name, String kind, String group, boolean significant, boolean loadOnBuild, String valueType, String description, String defaultValue) {
        this.name = name;
        this.kind = kind;
        this.group = group;
        this.significant = significant;
        this.loadOnBuild = loadOnBuild;
        this.valueType = valueType;
        this.description = description;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public String getKind() {
        return kind;
    }

    public String getGroup() {
        return group;
    }

    public boolean isSignificant() {
        return significant;
    }

    public boolean isLoadOnBuild() {
        return loadOnBuild;
    }

    public String getValueType() {
        return valueType;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultValue() {
        return defaultValue;
    }
}
