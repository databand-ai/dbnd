/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.log;

/**
 * Configuration for logDatasetOperation request.
 */
public class LogDatasetRequest {

    private Boolean withHistograms = null;
    private Boolean withPartition = null;
    private Boolean withStats = true;
    private Boolean withPreview = false;
    private Boolean withSchema = true;

    public LogDatasetRequest withHistograms() {
        return withHistograms(true);
    }

    public LogDatasetRequest withHistograms(Boolean withHistograms) {
        this.withHistograms = withHistograms;
        return this;
    }

    public LogDatasetRequest withPartition() {
        return withPartition(true);
    }

    public LogDatasetRequest withPartition(Boolean withPartition) {
        this.withPartition = withPartition;
        return this;
    }

    public LogDatasetRequest withStats() {
        return withStats(true);
    }

    public LogDatasetRequest withStats(Boolean withStats) {
        this.withStats = withStats;
        return this;
    }

    public LogDatasetRequest withPreview() {
        return withPreview(true);
    }

    public LogDatasetRequest withPreview(Boolean withPreview) {
        this.withPreview = withPreview;
        return this;
    }

    public LogDatasetRequest withSchema() {
        return withSchema(true);
    }

    public LogDatasetRequest withSchema(Boolean withSchema) {
        this.withSchema = withSchema;
        return this;
    }

    public Boolean getWithHistograms() {
        return withHistograms;
    }

    public Boolean getWithPartition() {
        return withPartition;
    }

    public Boolean getWithStats() {
        return withStats;
    }

    public Boolean getWithPreview() {
        return withPreview;
    }

    public Boolean getWithSchema() {
        return withSchema;
    }
}
