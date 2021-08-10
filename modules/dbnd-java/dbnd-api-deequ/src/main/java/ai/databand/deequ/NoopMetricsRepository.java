package ai.databand.deequ;

import com.amazon.deequ.analyzers.runners.AnalyzerContext;
import com.amazon.deequ.repository.MetricsRepository;
import com.amazon.deequ.repository.MetricsRepositoryMultipleResultsLoader;
import com.amazon.deequ.repository.ResultKey;
import scala.Option;

/**
 * Default noop deequ metrics repository.
 */
public class NoopMetricsRepository implements MetricsRepository {

    @Override
    public void save(ResultKey resultKey, AnalyzerContext analyzerContext) {

    }

    @Override
    public Option<AnalyzerContext> loadByKey(ResultKey resultKey) {
        return Option.empty();
    }

    @Override
    public MetricsRepositoryMultipleResultsLoader load() {
        return null;
    }
}
