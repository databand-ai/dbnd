import pytest

from dbnd._core.utils.string_utils import (
    clean_job_name,
    merge_dbnd_and_spark_logs,
    strip_by,
)


class TestStringUtils(object):
    def test_clean_job_name_1(self):
        assert clean_job_name("Aa[]1111") == "aa_1111"

    def test_clean_job_name_2(self):
        assert clean_job_name("AaBb[]1111", placeholder=r"-") == "aa-bb-1111"

    def test_clean_job_name_3(self):
        assert clean_job_name("AaBb[]1111", placeholder=r"-") == "aa-bb-1111"

    def test_logs_merging(self):
        dbnd = [
            "[2020-05-07 17:47:07,768] {tracking_store_console.py:89} INFO - \n",
            "dbnd one\n",
            "dbnd two\n",
            "[2020-05-07 17:47:09,231] {tracking_store_console.py:89} INFO - \n",
            "dbnd three \n",
            "dbnd four\n",
            "[2020-05-07 17:47:19,231] {tracking_store_console.py:89} INFO - \n",
            "dbnd string five \n",
            "dbnd string six\n",
        ]
        spark = [
            "[2020-05-07 17:47:07,881] INFO - Spark one\n",
            "[2020-05-07 17:47:07,882] INFO - Spark two\n",
            "[2020-05-07 17:47:09,811] INFO - Spark three\n",
            "[2020-05-07 17:47:09,812] INFO - Spark four\n",
            "[2020-05-07 17:47:19,881] INFO - Spark five\n",
        ]

        expected = [
            "[2020-05-07 17:47:07,768] {tracking_store_console.py:89} INFO - \n",
            "dbnd one\n",
            "dbnd two\n",
            "[2020-05-07 17:47:07,881] INFO - Spark one\n",
            "[2020-05-07 17:47:07,882] INFO - Spark two\n",
            "[2020-05-07 17:47:09,231] {tracking_store_console.py:89} INFO - \n",
            "dbnd three \n",
            "dbnd four\n",
            "[2020-05-07 17:47:09,811] INFO - Spark three\n",
            "[2020-05-07 17:47:09,812] INFO - Spark four\n",
            "[2020-05-07 17:47:19,231] {tracking_store_console.py:89} INFO - \n",
            "dbnd string five \n",
            "dbnd string six\n",
            "[2020-05-07 17:47:19,881] INFO - Spark five\n",
        ]

        result = merge_dbnd_and_spark_logs(dbnd, spark)
        assert result == expected

    @pytest.mark.parametrize(
        "predicate, input_str, expected",
        [
            (
                lambda c: not c.isalnum(),
                "...123123.123213.123..asd.22..",
                "123123.123213.123..asd.22",
            ),
            (lambda c: not c.isalpha(), "...123123.123213.123..asd.22..", "asd"),
        ],
    )
    def test_strip_by(self, predicate, input_str, expected):
        assert strip_by(predicate, input_str) == expected
