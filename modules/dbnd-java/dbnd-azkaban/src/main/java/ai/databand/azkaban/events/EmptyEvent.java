/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban.events;

import ai.databand.azkaban.AzkabanEvent;

public class EmptyEvent implements AzkabanEvent {
    @Override
    public void track() {
        // do nothing
    }
}
