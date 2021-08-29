package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Job {

    private String name;
    private String user;
    private String latestRunUid;
    private String latestRootTaskRunUid;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getLatestRunUid() {
        return latestRunUid;
    }

    public void setLatestRunUid(String latestRunUid) {
        this.latestRunUid = latestRunUid;
    }

    public String getLatestRootTaskRunUid() {
        return latestRootTaskRunUid;
    }

    public void setLatestRootTaskRunUid(String latestRootTaskRunUid) {
        this.latestRootTaskRunUid = latestRootTaskRunUid;
    }
}
