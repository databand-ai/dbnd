package ai.databand.azkaban.events;

import ai.databand.azkaban.AzkabanEvent;
import azkaban.event.Event;

public class DefaultEvent implements AzkabanEvent {

    public DefaultEvent(Event event) {
        // do nothing
    }

    @Override
    public void track() {
        // do nothing
    }
}
