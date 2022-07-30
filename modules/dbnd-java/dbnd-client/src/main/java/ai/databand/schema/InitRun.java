/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

public class InitRun {

    private final InitRunArgs initArgs;

    public InitRun(InitRunArgs initRunArgs) {
        this.initArgs = initRunArgs;
    }

    public InitRunArgs getInitArgs() {
        return initArgs;
    }
}
