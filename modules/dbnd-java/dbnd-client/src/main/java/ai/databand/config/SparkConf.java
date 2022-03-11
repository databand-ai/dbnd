package ai.databand.config;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Spark config properties source. Values are passed in uppercase+underscore format.
 */
public class SparkConf implements PropertiesSource {

    private static final Logger LOG = LoggerFactory.getLogger(SparkConf.class);

    private final Map<String, String> props;

    public SparkConf(PropertiesSource parent) {
        Map<String, String> sparkConf;
        try {
            SparkSession sparkSession = SparkSession.active();
            sparkConf = (Map<String, String>) JavaConverters.mapAsJavaMapConverter(sparkSession.conf().getAll()).asJava();
        } catch (Exception e) {
            LOG.warn("Databand is unable to resolve active spark session, 'spark.env.DBND...' variables won't be parsed");
            sparkConf = Collections.emptyMap();
        }
        Map<String, String> sparkProps = new HashMap<>(1);
        props = new HashMap<>(parent.values());
        for (Map.Entry<String, String> next : sparkConf.entrySet()) {
            if (next.getKey().startsWith("spark.env.")) {
                sparkProps.put(next.getKey().replace("spark.env.", ""), next.getValue());
            }
        }
        props.putAll(new NormalizedProps(sparkProps).values());
    }

    public Map<String, String> values() {
        return Collections.unmodifiableMap(props);
    }

    @Override
    public Optional<String> getValue(String key) {
        return Optional.ofNullable(props.get(key));
    }

}
