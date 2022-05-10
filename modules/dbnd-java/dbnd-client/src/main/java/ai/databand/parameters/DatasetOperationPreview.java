package ai.databand.parameters;

import ai.databand.schema.ColumnStats;
import ai.databand.schema.DatasetOperationSchema;
import ai.databand.schema.Pair;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static ai.databand.DbndPropertyNames.DBND_INTERNAL_ALIAS;

public class DatasetOperationPreview extends DatasetPreview {

    @Override
    public Object schema(Dataset<Row> input) {
        Dataset<?> schemaAlias = input.alias(String.format("%s_%s", DBND_INTERNAL_ALIAS, "SCHEMA"));
        return extractSchema(schemaAlias.schema(), schemaAlias.count()).left();
    }

    public Pair<String, List<Long>> extractSchema(StructType schema, long rows) {
        try {
            List<String> columns = new ArrayList<>(schema.fields().length);
            Map<String, String> dtypes = new HashMap<>(schema.fields().length);
            for (StructField field : schema.fields()) {
                columns.add(field.name());
                dtypes.put(field.name(), field.dataType().typeName());
            }
            List<Long> shape = Arrays.asList(rows, (long) columns.size());
            try {
                return new Pair<>(new ObjectMapper().writeValueAsString(new DatasetOperationSchema(columns, dtypes, shape)), shape);
            } catch (JsonProcessingException e) {
                return new Pair<>("", shape);
            }
        } catch (Exception e) {
            return new Pair<>("", Collections.emptyList());
        }
    }

    public List<ColumnStats> extractColumnStats(StructType schema, long rows) {
        return Arrays.stream(schema.fields())
            .map(field -> new ColumnStats()
                .setColumnName(field.name())
                .setColumnType(field.dataType().typeName())
                .setRecordsCount(rows))
            .collect(Collectors.toList());
    }
}
