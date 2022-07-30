/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban;

import ai.databand.azkaban.events.DefaultEvent;
import ai.databand.azkaban.events.FlowFinishedEvent;
import ai.databand.azkaban.events.FlowStartedEvent;
import ai.databand.azkaban.events.JobFinishedEvent;
import ai.databand.azkaban.events.JobStartedEvent;
import azkaban.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DbndEventReporter {

    private static final Logger LOG = LoggerFactory.getLogger(DbndEventReporter.class);

    private final Map<Event.Type, Class<? extends AzkabanEvent>> events;

    public DbndEventReporter() {
        events = new HashMap<>(1);
        events.put(Event.Type.FLOW_STARTED, FlowStartedEvent.class);
        events.put(Event.Type.FLOW_FINISHED, FlowFinishedEvent.class);
        events.put(Event.Type.JOB_STARTED, JobStartedEvent.class);
        events.put(Event.Type.JOB_FINISHED, JobFinishedEvent.class);
    }

    public boolean report(Event reportedEvent) {
        try {
            Class<? extends AzkabanEvent> type = events.getOrDefault(reportedEvent.getType(), DefaultEvent.class);
            AzkabanEvent event = type.getConstructor(Event.class).newInstance(reportedEvent);
            event.track();
        } catch (Throwable e) {
            LOG.error("Unable to track event", e);
        }
        return true;
    }

}
