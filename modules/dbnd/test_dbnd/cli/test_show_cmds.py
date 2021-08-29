from dbnd import dbnd_cmd


class TestShowCmds(object):
    def test_show_tasks(self):
        dbnd_cmd("show-tasks", [])

    def test_show_configs(self):
        dbnd_cmd("show-configs", [])
