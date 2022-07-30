/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban;

import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.EventType;
import azkaban.utils.Props;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DbndEventReporter implements AzkabanEventReporter {

    private static final Logger LOG = LoggerFactory.getLogger(DbndEventReporter.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<EventType, Class<? extends AzkabanEvent>> events;

    public DbndEventReporter(Props props) {
        events = new HashMap<>(1);
        events.put(EventType.FLOW_STARTED, FlowStartedEvent.class);
        events.put(EventType.FLOW_FINISHED, FlowFinishedEvent.class);
        events.put(EventType.JOB_STARTED, JobStartedEvent.class);
        events.put(EventType.JOB_FINISHED, JobFinishedEvent.class);
    }

    @Override
    public boolean report(EventType eventType, Map<String, String> map) {
        try {
            Class<? extends AzkabanEvent> type = events.getOrDefault(eventType, DefaultEvent.class);
            AzkabanEvent event = mapper.convertValue(map, type);
            event.track();
        } catch (Exception e) {
            LOG.error("Unable to track event", e);
        }
        return true;
    }
}
