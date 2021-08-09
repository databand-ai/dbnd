from pytest import fixture, raises

from dbnd import PipelineTask, output, task


@task
def task_fail_on_2(i):
    # type: (int) -> str
    if i % 2:
        raise TypeError("Some user error")
    return str(i)


class FailFastPipeline(PipelineTask):
    out_a = output.data

    def band(self):
        self.out_a = [
            task_fail_on_2(task_name="task_%s" % (bool(i % 2)), i=i) for i in range(10)
        ]


@fixture
def databand_context_kwargs():
    return dict(conf={"run": {"fail_fast": "True"}})


def test_fail_fast():
    with raises(Exception):
        FailFastPipeline().dbnd_run()

    # assert not pipe.out_b.exists()
