from dbnd import dbnd_run_cmd


class TestInlineCliCalls(object):
    def test_sanity_check(self):
        dbnd_run_cmd(["dbnd_sanity_check"])
