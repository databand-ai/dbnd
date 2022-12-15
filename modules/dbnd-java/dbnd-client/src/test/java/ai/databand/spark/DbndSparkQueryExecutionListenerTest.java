/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.spark;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import scala.collection.immutable.Seq;

import java.util.Collections;

import static ai.databand.DbndPropertyNames.DBND_INTERNAL_ALIAS;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DbndSparkQueryExecutionListenerTest {

    private DbndSparkQueryExecutionListener dbndSparkQueryExecutionListener = new DbndSparkQueryExecutionListener();
    private QueryExecution queryExecutionMock = Mockito.mock(QueryExecution.class);
    private LogicalPlan logicalPlanMock = Mockito.mock(LogicalPlan.class);
    private Seq<LogicalPlan> logicalPlanSeqMock = Mockito.mock(Seq.class);
    private LogicalPlan dbndAliasPlanMock = Mockito.mock(LogicalPlan.class);
    private WholeStageCodegenExec wholeStageCodegenExecMock = Mockito.mock(WholeStageCodegenExec.class);
    private DbndSparkQueryExecutionListener dbndSparkQueryExecutionListenerSpy = spy(dbndSparkQueryExecutionListener);

    @BeforeAll
    public void setup() {
        spy(dbndSparkQueryExecutionListener);
        when(queryExecutionMock.analyzed()).thenReturn(logicalPlanMock);
        when(queryExecutionMock.executedPlan()).thenReturn(wholeStageCodegenExecMock);
        when(logicalPlanMock.children()).thenReturn(logicalPlanSeqMock);
        when(logicalPlanSeqMock.apply(0)).thenReturn(dbndAliasPlanMock);
    }

    @Test
    public void testDbndInternalQueryExecution() {
        when(dbndAliasPlanMock.verboseString()).thenReturn(DBND_INTERNAL_ALIAS);

        dbndSparkQueryExecutionListenerSpy.onSuccess("mock", queryExecutionMock, 10000);
        verify(dbndSparkQueryExecutionListenerSpy, never()).getAllChildren(anyObject());
    }

    @Test
    public void testQueryExecution() {
        when(dbndAliasPlanMock.verboseString()).thenReturn("any");
        doReturn(Collections.EMPTY_LIST).when(dbndSparkQueryExecutionListenerSpy).getAllChildren(wholeStageCodegenExecMock);

        dbndSparkQueryExecutionListenerSpy.onSuccess("mock", queryExecutionMock, 10000);
        verify(dbndSparkQueryExecutionListenerSpy, times(1)).getAllChildren(anyObject());
    }

    @Test
    public void testExctactPath() {
        MatcherAssert.assertThat(
            "Dataset path should be properly extracted",
            dbndSparkQueryExecutionListener.exctractPath("InMemoryFileIndex(1 paths)[dbfs:/data/daily_data.csv]"),
            Matchers.equalTo("dbfs:/data/daily_data.csv")
        );
        MatcherAssert.assertThat(
            "Dataset path should be properly extracted",
            dbndSparkQueryExecutionListener.exctractPath("InMemoryFileIndex[dbfs:/data/daily_data.csv]"),
            Matchers.equalTo("dbfs:/data/daily_data.csv")
        );
        MatcherAssert.assertThat(
            "Dataset path should be properly extracted",
            dbndSparkQueryExecutionListener.exctractPath("s3:/data/daily_data.csv"),
            Matchers.equalTo("s3:/data/daily_data.csv")
        );
    }

}
