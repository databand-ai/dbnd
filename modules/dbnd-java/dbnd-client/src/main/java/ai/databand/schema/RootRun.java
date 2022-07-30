/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

public class RootRun {

    private final String rootRunUrl;

    private final String rootTaskRunUid;

    private final String rootRunUid;

    private final String rootTaskRunAttemptUid;


    public RootRun(String rootRunUrl, String rootTaskRunUid, String rootRunUid, String rootTaskRunAttemptUid) {
        this.rootRunUrl = rootRunUrl;
        this.rootTaskRunUid = rootTaskRunUid;
        this.rootRunUid = rootRunUid;
        this.rootTaskRunAttemptUid = rootTaskRunAttemptUid;
    }

    public String getRootRunUrl() {
        return rootRunUrl;
    }

    public String getRootTaskRunUid() {
        return rootTaskRunUid;
    }

    public String getRootRunUid() {
        return rootRunUid;
    }

    public String getRootTaskRunAttemptUid() {
        return rootTaskRunAttemptUid;
    }
}
