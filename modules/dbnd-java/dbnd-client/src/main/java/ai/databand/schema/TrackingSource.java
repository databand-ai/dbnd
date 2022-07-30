/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

public class TrackingSource {

    private final String name;
    private final String url;
    private final String env;
    private final String sourceType;
    private final String sourceInstanceUid;

    /**
     * Default constructor.
     *
     * @param name              tracking source name
     * @param url               tracking source url
     * @param env               tracking source env
     * @param sourceType        tracking source type, e.g. "azkaban" or "airflow"
     * @param sourceInstanceUid unique tracking source UID
     */
    public TrackingSource(String name,
                          String url,
                          String env,
                          String sourceType,
                          String sourceInstanceUid) {
        this.name = name;
        this.url = url;
        this.env = env;
        this.sourceType = sourceType;
        this.sourceInstanceUid = sourceInstanceUid;
    }

    public TrackingSource(AirflowTaskContext airflowTaskContext) {
        this(airflowTaskContext.getDagId(),
            airflowTaskContext.getAirflowName(),
            "airflow",
            "airflow",
            airflowTaskContext.getAirflowInstanceUid()
        );
    }

    public TrackingSource(AzkabanTaskContext azkabanTaskContext) {
        this(azkabanTaskContext.azkabanInstanceId(),
            azkabanTaskContext.azkabanUrl(),
            "azkaban",
            "azkaban",
            azkabanTaskContext.azkabanInstanceUuid()
        );
    }

    public String getName() {
        return name;
    }

    public String getUrl() {
        return url;
    }

    public String getEnv() {
        return env;
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getSourceInstanceUid() {
        return sourceInstanceUid;
    }
}
