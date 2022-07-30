/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import java.util.List;

public class LogTargets {

    private final List<LogTarget> targetsInfo;

    public LogTargets(List<LogTarget> targetsInfo) {
        this.targetsInfo = targetsInfo;
    }

    public List<LogTarget> getTargetsInfo() {
        return targetsInfo;
    }
}
