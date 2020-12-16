from dbnd import Config, new_dbnd_context, parameter


class DummyConfig(Config):
    _conf__task_family = "my_dummy"

    foo = parameter(default="foofoo")[str]
    bar = parameter(default="barbar")[str]


def test_single_instance():
    with new_dbnd_context(name="first") as ctx:
        config1 = ctx.settings.get_config("my_dummy")
        config2 = Config.current("my_dummy")
        config3 = DummyConfig.current()
        config4 = ctx.settings.get_config("my_dummy")
        assert config1 is config2
        assert config1 is config3
        assert config1 is config4

        config1.foo = "123"

        assert config2.foo == "123"
        assert config3.foo == "123"
        assert config4.foo == "123"


def test_different_instances():
    with new_dbnd_context(name="first") as ctx:
        config1 = ctx.settings.get_config("my_dummy")
        with new_dbnd_context(name="second") as ctx2:
            config2 = ctx2.settings.get_config("my_dummy")
            assert config1 is not config2
