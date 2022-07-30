/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban.links;

import java.util.Collections;
import java.util.Map;

public class EmptyAzkabanLinks implements AzkabanLinks {

    @Override
    public Map<String, String> flowLinks() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> jobLinks(String jobId) {
        return Collections.emptyMap();
    }
}
