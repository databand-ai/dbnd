package ai.databand.azkaban;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultEvent implements AzkabanEvent {

    @Override
    public void track() {
        // do nothing
    }
}
