# © Copyright Databand.ai, an IBM Company 2022


class TestTarget(object):
    def test_http_to_wasb(self):
        from targets.marshalling.spark import _convert_http_to_wasb_for_azure

        local_target = "dbnd_examples/data/people.txt"

        azure_http_target = "http://account_name.blob.core.windows.net/container/file/"
        azure_wasb_target = "wasb://container@account_name.blob.core.windows.net/file/"

        azure_https_target = (
            "https://account_name.blob.core.windows.net/container/file/"
        )
        azure_wasbs_target = (
            "wasbs://container@account_name.blob.core.windows.net/file/"
        )

        assert _convert_http_to_wasb_for_azure(azure_http_target) == azure_wasb_target
        assert _convert_http_to_wasb_for_azure(azure_https_target) == azure_wasbs_target
        assert _convert_http_to_wasb_for_azure(local_target) == local_target
