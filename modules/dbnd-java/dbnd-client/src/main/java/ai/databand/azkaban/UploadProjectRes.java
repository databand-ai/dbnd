/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban;

public class UploadProjectRes {

    private String error;
    private String projectId;
    private String version;

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
