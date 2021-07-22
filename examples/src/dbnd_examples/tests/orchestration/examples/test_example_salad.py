import pytest
import six

from pytest import fixture

from dbnd import relative_path
from dbnd.testing.helpers import run_test_notebook
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.data import data_repo
from dbnd_examples.orchestration.examples.salad import salad
from dbnd_examples.orchestration.examples.salad.salad import prepare_salad
from dbnd_examples.orchestration.examples.salad.salad_classes import (
    LunchWithSalad,
    PrepareSalad,
)
from targets import target


class TestSaladExample(object):
    def test_salad_local__prepare(self):
        salad = PrepareSalad()
        assert_run_task(salad)

    def test_salad_deco_local__prepare(self):
        salad = prepare_salad.t()
        assert_run_task(salad)

    def test_salad_local__lunch(self):
        lunch = LunchWithSalad()
        assert_run_task(lunch)

    def test_prepare_salad_deco(self,):
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
