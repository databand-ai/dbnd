from dbnd._core.utils.string_utils import (
    clean_job_name,
    clean_job_name_dns1123,
    merge_dbnd_and_spark_logs,
)


class TestStringUtils(object):
    def test_clean_job_name_1(self):
        assert clean_job_name("Aa[]1111") == "aa_1111"

    def test_clean_job_name_2(self):
        assert clean_job_name("AaBb[]1111", placeholder=r"-") == "aa-bb-1111"

    def test_clean_job_name_3(self):
        assert clean_job_name("AaBb[]1111", placeholder=r"-") == "aa-bb-1111"

    def test_clean_job_name_postfix_1(self):
        job_id = "6a8330cc"
        assert (
            clean_job_name_dns1123("AaBb[]1111.jobname", postfix=".%s" % job_id)
            == "aa-bb-1111.jobname.6a8330cc"
        )

    def test_clean_job_name_postfix_characters(self):
        job_id = "6a8330cc"
        assert (
            clean_job_name_dns1123(
                "dbnd.tttttttt-operator.t-t-t-training-with-t-sssss-session-",
                postfix=".%s" % job_id,
            )
            == "dbnd.tttttttt-operator.t-t-t-training-with-t-sssss-session.6a8330cc"
        )

    def test_clean_job_name_postfix_2(self):
        job_id = "6a8330cc"
        assert (
            clean_job_name_dns1123(
                "driver_submit__9991469ce9.BashCmd", postfix=".%s" % job_id
            )
            == "driver-submit-9991469ce9.bash-cmd.6a8330cc"
        )

    def test_clean_job_name_postfix_max(self):
        job_id = "6a8330cc"
        assert (
            clean_job_name_dns1123("a" * 300, placeholder=r"-", postfix=".%s" % job_id)
            == "a" * 244 + ".6a8330cc"
        )

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
