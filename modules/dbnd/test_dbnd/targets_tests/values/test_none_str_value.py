from dbnd import PipelineTask, Task, parameter
from dbnd._core.current import try_get_databand_context


class NoneStringPipeline(PipelineTask):
    def band(self):
        result = SetNoneString()
        return CheckNoneString(none_token=result.none_token)


class SetNoneString(Task):
    none_token = parameter.output.txt[str]

    def run(self):
        self.none_token = None


class CheckNoneString(Task):
    none_token = parameter(default=None)[str]

    def run(self):
        assert not self.none_token


class TestNoneStringType:
    def test_none_string_marshalling(self):
        dc = try_get_databand_context()
        # Prevent target caching, force reload from disk
        dc.settings.run.target_cache_on_access = False
        p = NoneStringPipeline()
        p.dbnd_run()
