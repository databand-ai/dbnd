/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.examples;

import ai.databand.schema.DatasetOperationType;
import ai.databand.spark.DbndSparkQueryExecutionListener;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeUnit;

/**
 * This is "slow" implementation of Query Listener which is used in tests to emulate delays in query processing.
 * Spark listeners are processing event asynchronously which can lead to race conditions.
 * E.g.: DBND run was completed, but Listener is still busy processing events.
 * We handle such cases, and this implementation allows us to "simulate" them.
 */
public class SlowDbndSparkQueryExecutionListener extends DbndSparkQueryExecutionListener {

    @Override
    protected void log(String path, DatasetOperationType operationType, StructType datasetSchema, long rows,boolean isPartitioned) {
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        super.log(path, operationType, datasetSchema, rows,isPartitioned);
    }
}
