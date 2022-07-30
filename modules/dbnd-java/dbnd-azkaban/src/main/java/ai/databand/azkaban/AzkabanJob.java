/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban;

import java.time.ZonedDateTime;

public abstract class AzkabanJob {

    public abstract String state();

    public abstract String log();

    public abstract boolean isFailed();

    public abstract ZonedDateTime startDate();
}
