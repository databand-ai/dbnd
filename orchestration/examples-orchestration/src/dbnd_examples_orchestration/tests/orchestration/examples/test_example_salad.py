# © Copyright Databand.ai, an IBM Company 2022

from dbnd_examples_orchestration.data import data_repo
from dbnd_examples_orchestration.orchestration.examples.salad import prepare_salad
from pytest import fixture

from dbnd_run.testing.helpers import assert_run_task
from targets import target


class TestSaladExample(object):
    def test_salad_deco_local__prepare(self):
        salad = prepare_salad.t()
        assert_run_task(salad)

    def test_prepare_salad_deco(self):
        assert_run_task(prepare_salad.t(vegetables=data_repo.vegetables))

    @fixture
    def vegetables(self, tmpdir):
        t = target(str(tmpdir), "vegetables.csv")
        t.write("a\nb\n")
        return t

    def test_regular_invoke(self, vegetables):
        salad = prepare_salad(vegetables.readlines(), "mayo")
        assert len(salad) == 2

    def test_as_task(self, vegetables):
        assert_run_task(prepare_salad.t(vegetables=vegetables))
