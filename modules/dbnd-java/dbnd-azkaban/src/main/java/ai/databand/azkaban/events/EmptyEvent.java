package ai.databand.azkaban.events;

import ai.databand.azkaban.AzkabanEvent;

public class EmptyEvent implements AzkabanEvent {
    @Override
    public void track() {
        // do nothing
    }
}
