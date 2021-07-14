package ai.databand.schema;

import ai.databand.schema.jackson.ZonedDateTimeDeserializer;
import ai.databand.schema.jackson.ZonedDateTimeSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.ZonedDateTime;

public class TaskRunEnv {

    private String userData;

    private String uid;

    private String user;

    private String userCodeVersion;

    private String machine;

    private String cmdLine;

    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    private ZonedDateTime heartbeat;

    private String databand_version;

    private String projectRoot;

    private boolean userCodeCommitted;

    public TaskRunEnv(String userData, String uid, String user, String userCodeVersion, String machine, String cmdLine, ZonedDateTime heartbeat, String databand_version, String projectRoot, boolean userCodeCommitted) {
        this.userData = userData;
        this.uid = uid;
        this.user = user;
        this.userCodeVersion = userCodeVersion;
        this.machine = machine;
        this.cmdLine = cmdLine;
        this.heartbeat = heartbeat;
        this.databand_version = databand_version;
        this.projectRoot = projectRoot;
        this.userCodeCommitted = userCodeCommitted;
    }

    public String getUserData() {
        return userData;
    }

    public String getUid() {
        return uid;
    }

    public String getUser() {
        return user;
    }

    public String getUserCodeVersion() {
        return userCodeVersion;
    }

    public String getMachine() {
        return machine;
    }

    public String getCmdLine() {
        return cmdLine;
    }

    public ZonedDateTime getHeartbeat() {
        return heartbeat;
    }

    public String getDataband_version() {
        return databand_version;
    }

    public String getProjectRoot() {
        return projectRoot;
    }

    public boolean isUserCodeCommitted() {
        return userCodeCommitted;
    }
}
