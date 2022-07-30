/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.deequ;

import com.amazon.deequ.repository.ResultKey;
import scala.collection.immutable.Map;

public class DbndResultKey extends ResultKey {

    private final String dataSetName;

    public DbndResultKey(long dataSetDate, Map<String, String> tags, String dataSetName) {
        super(dataSetDate, tags);
        this.dataSetName = dataSetName;
    }

    public DbndResultKey(String dataSetName) {
        super(System.currentTimeMillis(), scala.collection.immutable.Map$.MODULE$.<String, String>empty());
        this.dataSetName = dataSetName;
    }

    public DbndResultKey(long dataSetDate, Map<String, String> tags) {
        super(dataSetDate, tags);
        this.dataSetName = "dataSet";
    }

    public String dataSetName() {
        return dataSetName;
    }
}
