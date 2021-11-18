package ai.databand.schema;


import java.util.List;
import java.util.Map;

public class DatasetOperationSchema {

    private final List<String> columns;
    private final Map<String, String> dtypes;
    private final List<Long> shape;
    private static final String SPARK_DATAFRAME_TYPE = "Spark.DataFrame";

    public DatasetOperationSchema(List<String> columns, Map<String, String> dtypes, List<Long> shape) {
        this.columns = columns;
        this.dtypes = dtypes;
        this.shape = shape;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Map<String, String> getDtypes() {
        return dtypes;
    }

    public List<Long> getShape() {
        return shape;
    }

    public String getType() {
        return SPARK_DATAFRAME_TYPE;
    }
}
