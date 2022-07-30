/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban.links;

import java.util.Map;

public interface AzkabanLinks {

    Map<String, String> flowLinks();

    Map<String, String> jobLinks(String jobId);

}
