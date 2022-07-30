/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban;

import ai.databand.config.DbndConfig;
import ai.databand.schema.AzkabanTaskContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.databand.DbndPropertyNames.DBND__AZKABAN__SYNC_FLOWS;
import static ai.databand.DbndPropertyNames.DBND__AZKABAN__SYNC_PROJECTS;

public class AzkabanDbndConfig {

    private final Set<String> projectsToTrack;
    private final Set<String> flowsToTrack;

    public AzkabanDbndConfig(DbndConfig dbndConfig) {
        this(dbndConfig.values());
    }

    public AzkabanDbndConfig(Map<String, String> props) {
        this(
            props.get(DBND__AZKABAN__SYNC_PROJECTS),
            props.get(DBND__AZKABAN__SYNC_FLOWS)
        );
    }

    public AzkabanDbndConfig(String syncProjects, String syncFlows) {
        projectsToTrack = syncProjects == null || syncProjects.isEmpty()
            ? Collections.emptySet()
            : Arrays.stream(syncProjects.split(",")).map(String::toLowerCase).collect(Collectors.toSet());
        flowsToTrack = syncFlows == null || syncFlows.isEmpty()
            ? Collections.emptySet()
            : Arrays.stream(syncFlows.split(",")).map(String::toLowerCase).collect(Collectors.toSet());
    }

    /**
     * If projects to track are empty — we track everything.
     * If flows to track also empty — we track everything.
     * Then we check if either project of flow are specified in list.
     *
     * @param projectName Azkaban project name
     * @param flowName    Azkaban flow name
     * @return true if tracking is enabled for given flow inside given project
     */
    public boolean isTrackingEnabled(String projectName, String flowName) {
        if (projectsToTrack.isEmpty() && flowsToTrack.isEmpty()) {
            return true;
        }
        if (!projectsToTrack.isEmpty() && !flowsToTrack.isEmpty()) {
            return projectsToTrack.contains(projectName.toLowerCase()) || flowsToTrack.contains(flowName.toLowerCase());
        }
        if (!projectsToTrack.isEmpty()) {
            return projectsToTrack.contains(projectName.toLowerCase());
        }
        return flowsToTrack.contains(flowName.toLowerCase());
    }

    /**
     * Convenient checking with task context.
     *
     * @param ctx Azkaban task context to check against
     * @return true is tracking is enabled for project and flow extracted from Azkaban task context
     */
    public boolean isTrackingEnabled(AzkabanTaskContext ctx) {
        return isTrackingEnabled(ctx.projectName(), ctx.flowId());
    }
}
