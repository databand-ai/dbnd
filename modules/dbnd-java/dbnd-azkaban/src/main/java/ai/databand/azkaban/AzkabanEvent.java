package ai.databand.azkaban;

/**
 * Wrapper for Azkaban events.
 */
public interface AzkabanEvent {

    /**
     * Track event in the Databand.
     */
    void track();

}
