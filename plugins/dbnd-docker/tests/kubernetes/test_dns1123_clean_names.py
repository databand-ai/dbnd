import re

import pytest

from dbnd_docker.kubernetes.dns1123_clean_names import (
    MAX_CLEAN_LABEL_NAME_DNS1123_LEN,
    MAX_CLEAN_SUBDOMAIN_NAME_DNS1123_LEN,
    clean_label_name_dns1123,
    clean_subdomain_name_dns1123,
)


class TestStringUtils(object):
    def test_clean_job_name_postfix_1(self):
        job_id = "6a8330cc"
        assert (
            clean_subdomain_name_dns1123("AaBb[]1111.jobname", postfix=".%s" % job_id)
            == "aa-bb-1111.jobname.6a8330cc"
        )

    def test_clean_job_name_postfix_characters(self):
        job_id = "6a8330cc"
        assert (
            clean_subdomain_name_dns1123(
                "dbnd.tttttttt-operator.t-t-t-training-with-t-sssss-session-",
                postfix=".%s" % job_id,
            )
            == "dbnd.tttttttt-operator.t-t-t-training-with-t-sssss-session.6a8330cc"
        )

    def test_clean_job_name_postfix_2(self):
        job_id = "6a8330cc"
        assert (
            clean_subdomain_name_dns1123(
                "driver_submit__9991469ce9.BashCmd", postfix=".%s" % job_id
            )
            == "driver-submit-9991469ce9.bash-cmd.6a8330cc"
        )

    def test_clean_job_name_postfix_max(self):
        job_id = "6a8330cc"
        assert (
            clean_subdomain_name_dns1123("a" * 300, postfix=".%s" % job_id)
            == "a" * 244 + ".6a8330cc"
        )

    @pytest.mark.parametrize(
        "input_value, func, max_size",
        [
            (
                "very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very"
                ".very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very"
                ".very.very.very.very.very.very.very.very.very.very.very.very.very.very.verylong.input_value.input_value"
                ".input_value.That.issimmilar.to-a.task_name.which.is_posible.inour.syste,32165478965321",
                clean_subdomain_name_dns1123,
                MAX_CLEAN_SUBDOMAIN_NAME_DNS1123_LEN,
            ),
            (
                "very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very"
                ".very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very.very"
                ".very.very.very.very.very.very.very.very.very.very.very.very.very.very.verylong.input_value.input_value"
                ".input_value.That.issimmilar.to-a.task_name.which.is_posible.inour.syste,32165478965321",
                clean_label_name_dns1123,
                MAX_CLEAN_LABEL_NAME_DNS1123_LEN,
            ),
            (
                "dbnd.collections-net-training-operator.customer.apps.services.orker.databand.operators.vecation.collection-net-working-operator.e88e01de-1",
                clean_label_name_dns1123,
                MAX_CLEAN_LABEL_NAME_DNS1123_LEN,
            ),
        ],
    )
    def test_clean_name_dns1123(self, input_value, func, max_size):
        """this test that no matter what the output is, the result is fit with the dns-1123 validation regex"""
        result = func(input_value)
        # this is a regex used by k8s to validate the right name for dns1123
        assert re.match(r"(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?", result)
        assert len(result) <= max_size
