---
"title": "Testing Spark Tasks"
---
It is possible to test Spark tasks using Pytest.

In the following example, we test the `word_count_inline` task to ensure it is running.
The `assert_run_task`, the function makes sure that the task has been completed and all outputs exist:

```python
import pytest

from dbnd.testing.helpers_pytest import assert_run_task

from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline


class TestSparkTasksLocally(object):
    @pytest.fixture(autouse=True)
    def spark_conn(self, spark_conn_str):
        self.spark_conn_str = spark_conn_str

    def test_spark_inline(self):
        assert_run_task(word_count_inline.t(text=__file__))
```