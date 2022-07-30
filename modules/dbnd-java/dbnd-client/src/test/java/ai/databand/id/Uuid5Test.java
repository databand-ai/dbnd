/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.id;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

class Uuid5Test {

    @Test
    public void testGenerateAfId() {
        String dag_id = "process_data_spark";
        String execution_date = "2020-09-03T00:00:00+00:00";
        String task_id = "spark_driver";
        String try_number = "1";
        String exceptedRootRunUid = "d7328053-cb5a-5825-b8ad-2fdcf396a0ad";
        String jobId = new Uuid5(Uuid5.NAMESPACE_DBND_RUN, String.format("%s:%s", dag_id, execution_date)).toString();
        MatcherAssert.assertThat("Wrong uuid5", jobId, Matchers.equalTo(exceptedRootRunUid));
    }

}
