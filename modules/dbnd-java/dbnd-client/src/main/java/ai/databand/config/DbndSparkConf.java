/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.config;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Spark config properties source. Values are passed in uppercase+underscore format.
 */
public class DbndSparkConf implements PropertiesSource {

    private static final Logger LOG = LoggerFactory.getLogger(DbndSparkConf.class);

    private final Map<String, String> props;

    public DbndSparkConf(PropertiesSource parent) {
        Map<String, String> sparkConf;
        try {
            sparkConf = this.sparkConfToMap(new SparkConf());
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

    /**
     * Puts spark application configuration in SparkConf object as key-value in map object
     *
     * @param sparkConf configuration for spark application
     * @return spark configuration as java map object
     */
    private Map<String, String> sparkConfToMap(SparkConf sparkConf) {
        Map<String, String> result = new HashMap<>();
        Arrays.stream(sparkConf.getAll())
            .forEach(x -> result.put(x._1(), x._2));
        return result;
    }

    public Map<String, String> values() {
        return Collections.unmodifiableMap(props);
    }

    @Override
    public Optional<String> getValue(String key) {
        return Optional.ofNullable(props.get(key));
    }

}
