from dbnd import (
    Task,
    get_remote_engine_name,
    get_task_params_defs,
    get_task_params_values,
    parameter,
    task,
)


@task()
def access_engine_name_mid_run():
    return get_remote_engine_name()


class DummyTask(Task):
    a = parameter(default=1)[int]
    b = parameter(default=None)[str]
    c = parameter(default=1.2)[float]

    def run(self):
        pass


class TestAccessHelpers(object):
    def test_get_remote_engine_name(self):
        run = access_engine_name_mid_run.dbnd_run()
        assert run.load_from_result() == "local_machine_engine"

    def test_get_remote_engine_name_without_run(self):
        assert get_remote_engine_name() is None

    def test_get_task_params(self):
        t = DummyTask(b="boom")
        params = get_task_params_values(t)
        assert all(
            k in params and params[k] == v
            for k, v in [("a", 1), ("b", "boom"), ("c", 1.2)]
        )

    def test_get_task_params_defs(self):
        t = DummyTask()
        params_defs = get_task_params_defs(t)
        assert all(k in params_defs for k in ["a", "b", "c"])

    def test_get_task_cls_params_defs(self):
        params_defs = get_task_params_defs(DummyTask)
        assert all(k in params_defs for k in ["a", "b", "c"])
