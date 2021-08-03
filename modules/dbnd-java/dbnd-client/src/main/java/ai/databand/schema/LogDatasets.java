package ai.databand.schema;

import java.util.List;

public class LogDatasets {

    private final List<LogDataset> datasetsInfo;

    public LogDatasets(List<LogDataset> datasetsInfo) {
        this.datasetsInfo = datasetsInfo;
    }

    public List<LogDataset> getDatasetsInfo() {
        return datasetsInfo;
    }
}
