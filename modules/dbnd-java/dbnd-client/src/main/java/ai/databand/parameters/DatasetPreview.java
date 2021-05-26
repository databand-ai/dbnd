package ai.databand.parameters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

public class DatasetPreview implements TaskParameterPreview<Dataset<Row>> {

    @Override
    public String compact(Dataset<Row> input) {
        return "Dataset";
    }

    @Override
    public String full(Dataset<Row> input) {
        try {
            return input.showString(20, 2048, false);
        } catch (Exception e) {
            return "";
        }
    }

    @Override
    public String typeName(Class<Dataset<Row>> input) {
        return "Dataset";
    }

    @Override
    public String schema(Dataset<Row> input) {
        return input.schema().prettyJson();
    }

    /**
     * This method calculates the exact size of the dataframe.
     * TODO: opt-out of dimensions calculation because count() kicks out new job and it may take a lot of time.
     * There is no easy way to calculate dataframe size without converting to RDD and using rdd.estimateCount()
     *
     * @param input
     * @return
     */
    @Override
    public List<Long> dimensions(Dataset<Row> input) {
        long columns = input.columns().length;
        long rows = input.count();
        return Arrays.asList(rows, columns);
    }
}
