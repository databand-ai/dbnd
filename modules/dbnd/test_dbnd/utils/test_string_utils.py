from dbnd._core.utils.string_utils import clean_job_name, clean_job_name_dns1123


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
