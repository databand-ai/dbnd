package ai.databand.schema;

import java.util.Map;

public class SaveExternalLinks {

    private final String taskRunAttemptUid;
    private final Map<String, String> externalLinksDict;

    public SaveExternalLinks(String taskRunAttemptUid, Map<String, String> externalLinksDict) {
        this.taskRunAttemptUid = taskRunAttemptUid;
        this.externalLinksDict = externalLinksDict;
    }

    public String getTaskRunAttemptUid() {
        return taskRunAttemptUid;
    }

    public Map<String, String> getExternalLinksDict() {
        return externalLinksDict;
    }
}
