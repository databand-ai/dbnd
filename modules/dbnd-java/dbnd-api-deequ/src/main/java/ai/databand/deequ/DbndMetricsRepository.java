/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.deequ;

import ai.databand.DbndWrapper;
import com.amazon.deequ.analyzers.runners.AnalyzerContext;
import com.amazon.deequ.repository.MetricsRepository;
import com.amazon.deequ.repository.MetricsRepositoryMultipleResultsLoader;
import com.amazon.deequ.repository.ResultKey;
import scala.Option;
import scala.collection.JavaConverters;

import java.util.Map;

/**
 * Deequ metrics repository implementation. Reports all Deequ metrics to Databand.
 */
public class DbndMetricsRepository implements MetricsRepository {

    private final DbndWrapper dbnd;
    private final MetricsRepository origin;

    public DbndMetricsRepository(DbndWrapper dbnd) {
        this.dbnd = dbnd;
        this.origin = new NoopMetricsRepository();
    }

    public DbndMetricsRepository(DbndWrapper dbnd, MetricsRepository originRepo) {
        this.dbnd = dbnd;
        this.origin = originRepo;
    }

    public DbndMetricsRepository() {
        this.dbnd = DbndWrapper.instance();
        this.origin = new NoopMetricsRepository();
    }

    public DbndMetricsRepository(MetricsRepository originRepo) {
        this.dbnd = DbndWrapper.instance();
        this.origin = originRepo;
    }

    @Override
    public void save(ResultKey resultKey, AnalyzerContext analyzerContext) {
        origin.save(resultKey, analyzerContext);
        String dfName;
        if (resultKey instanceof DbndResultKey) {
            dfName = ((DbndResultKey) resultKey).dataSetName();
        } else {
            Map<String, String> tags = JavaConverters.mapAsJavaMapConverter(resultKey.tags()).asJava();
            dfName = tags.getOrDefault("name", "data");
        }

        DeequToDbnd converted = new DeequToDbnd(dfName, analyzerContext);

        dbnd.logMetrics(converted.metrics());
        dbnd.logHistogram(converted.histograms());
    }

    @Override
    public Option<AnalyzerContext> loadByKey(ResultKey resultKey) {
        return origin.loadByKey(resultKey);
    }

    @Override
    public MetricsRepositoryMultipleResultsLoader load() {
        return origin.load();
    }
}
