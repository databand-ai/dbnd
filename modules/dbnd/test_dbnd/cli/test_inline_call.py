from dbnd import dbnd_run_cmd


def _tc(key, value):
    return {"test_section": {key: value}}


class TestInlineCliCalls(object):
    def test_sanity_check(self):
        dbnd_run_cmd(["dbnd_sanity_check"])
