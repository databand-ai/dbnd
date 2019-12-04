from dbnd import auto_namespace, band, parameter
from dbnd._core.constants import CloudType
from test_dbnd.factories import FooConfig, TTask


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

        assert "from_config" == t.foo.bar
        assert "from_config" == t.foo.quz

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
        assert "first_pipeline.defaults.bar" == t_result.foo.bar
        assert "SecondTask.defaults.quz" == t_result.foo.quz

    def test_pipeline_2(self):
        first_pipeline, second = second_pipeline.t().result

        assert "first_pipeline.defaults.bar" == first_pipeline.task.foo.bar
        assert "second_pipeline.band.override.quz" == first_pipeline.task.foo.quz
