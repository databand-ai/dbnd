# © Copyright Databand.ai, an IBM Company 2022

from dbnd import auto_namespace, band, parameter
from dbnd_test_scenarios.test_common.task.factories import FooConfig, TTask


auto_namespace(scope=__name__)


class FirstTask(TTask):
    # exists only for local
    foo = parameter(default="FooConfig")[FooConfig]
    param = parameter(default="FirstTask.inline.param")[str]


class SecondTask(FirstTask):
    defaults = {
        FooConfig.bar: "SecondTask.defaults.bar",
        FooConfig.quz: "SecondTask.defaults.quz",
    }


@band(defaults={FooConfig.bar: "first_pipeline.defaults.bar"})
def first_pipeline():
    return SecondTask(param="first_pipeline.band.param").t_output


@band(defaults={FooConfig.quz: "second_pipeline.defaults.quz"})
def second_pipeline():
    return (
        first_pipeline(
            override={
                FooConfig.quz: "second_pipeline.band.override.quz"
            }  # overridden by self.config
        ),
        SecondTask().t_output,
    )


class TestSubConfigOverride(object):
    def test_simple(self):
        t = FirstTask()

        assert t.foo

        assert "from_constr" == t.foo.bar
        assert "from_constr" == t.foo.quz

    def test_not_created(self):
        t = FirstTask(foo=None)

        assert t.foo is None

    def test_inheritance(self):
        t = SecondTask()

        assert "SecondTask.defaults.bar" == t.foo.bar
        assert "SecondTask.defaults.quz" == t.foo.quz

    def test_pipeline(self):
        t = first_pipeline.t()
        t_result = t.result.task
        # secondtask defaults are applied later ->
        # they will override defaults from first_pipeline
        assert t_result.foo.bar == "SecondTask.defaults.bar"
        assert t_result.foo.quz == "SecondTask.defaults.quz"

    def test_pipeline_2(self):
        first_pipeline, second = second_pipeline.t().result

        assert first_pipeline.task.foo.bar == "SecondTask.defaults.bar"
        assert first_pipeline.task.foo.quz == "second_pipeline.band.override.quz"
