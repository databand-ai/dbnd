/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import java.util.List;

public class TaskDefinition {

    private final String name;
    private final String moduleSource;
    private final String sourceHash;
    private final String classVersion;
    private final String taskDefinitionUid;
    private final String moduleSourceHash;
    private final List<TaskParamDefinition> taskParamDefinitions;
    private final String family;
    private final String type;
    private final String source;

    public TaskDefinition(String name, String moduleSource, String sourceHash, String classVersion, String taskDefinitionUid, String moduleSourceHash, List<TaskParamDefinition> taskParamDefinitions, String family, String type, String source) {
        this.name = name;
        this.moduleSource = moduleSource;
        this.sourceHash = sourceHash;
        this.classVersion = classVersion;
        this.taskDefinitionUid = taskDefinitionUid;
        this.moduleSourceHash = moduleSourceHash;
        this.taskParamDefinitions = taskParamDefinitions;
        this.family = family;
        this.type = type;
        this.source = source;
    }

    public String getName() {
        return name;
    }

    public String getModuleSource() {
        return moduleSource;
    }

    public String getSourceHash() {
        return sourceHash;
    }

    public String getClassVersion() {
        return classVersion;
    }

    public String getTaskDefinitionUid() {
        return taskDefinitionUid;
    }

    public String getModuleSourceHash() {
        return moduleSourceHash;
    }

    public List<TaskParamDefinition> getTaskParamDefinitions() {
        return taskParamDefinitions;
    }

    public String getFamily() {
        return family;
    }

    public String getType() {
        return type;
    }

    public String getSource() {
        return source;
    }
}
