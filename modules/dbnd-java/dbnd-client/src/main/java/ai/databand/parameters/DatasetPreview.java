package ai.databand.parameters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

import static ai.databand.DbndPropertyNames.DBND_INTERNAL_ALIAS;

public class DatasetPreview implements TaskParameterPreview<Dataset<Row>> {

    @Override
    public String compact(Dataset<Row> input) {
        return "Dataset";
    }

    @Override
    public String full(Dataset<Row> input) {
        Dataset<?> previewAlias = input.alias(String.format("%s_%s",DBND_INTERNAL_ALIAS,"PREVIEW"));
        try {
            return previewAlias.showString(20, 2048, false);
        } catch (Exception e) {
            return "";
        }
    }

    @Override
    public String typeName(Class<Dataset<Row>> input) {
        return "Dataset";
    }

    @Override
    public Object schema(Dataset<Row> input) {
        Dataset<?> previewSchemaAlias = input.alias(String.format("%s_%s",DBND_INTERNAL_ALIAS,"PREVIEW_SCHEMA"));
        try {
            return previewSchemaAlias.schema().prettyJson();
        } catch (Exception e) {
            return "";
        }
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
        Dataset<?> dimsAlias = input.alias(String.format("%s_%s",DBND_INTERNAL_ALIAS,"DIMS"));
        try {
            long rows = dimsAlias.count();
            long columns = dimsAlias.columns().length;
            return Arrays.asList(rows, columns);
        } catch (Exception e) {
            return Arrays.asList(0L, 0L);
        }
    }
}
