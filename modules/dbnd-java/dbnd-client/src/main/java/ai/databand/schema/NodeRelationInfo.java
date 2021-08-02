package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeRelationInfo {

    private Integer id;
    private Integer downstreamTrId;
    private Integer upstreamTrId;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getDownstreamTrId() {
        return downstreamTrId;
    }

    public void setDownstreamTrId(Integer downstreamTrId) {
        this.downstreamTrId = downstreamTrId;
    }

    public Integer getUpstreamTrId() {
        return upstreamTrId;
    }

    public void setUpstreamTrId(Integer upstreamTrId) {
        this.upstreamTrId = upstreamTrId;
    }
}
