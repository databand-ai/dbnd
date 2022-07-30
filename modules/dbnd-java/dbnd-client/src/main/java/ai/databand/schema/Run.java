/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Run {

    private String rootRunUid;
    private String projectName;

    public String getRootRunUid() {
        return rootRunUid;
    }

    public void setRootRunUid(String rootRunUid) {
        this.rootRunUid = rootRunUid;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }
}
