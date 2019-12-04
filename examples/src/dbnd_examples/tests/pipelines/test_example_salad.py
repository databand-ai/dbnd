import pytest
import six

from pytest import fixture

from dbnd import relative_path
from dbnd.testing import assert_run_task
from dbnd.testing.helpers import run_test_notebook
from dbnd_examples.data import data_repo, dbnd_examples_data_path
from dbnd_examples.pipelines.salad import salad
from dbnd_examples.pipelines.salad.salad import prepare_salad
from dbnd_examples.pipelines.salad.salad_classes import LunchWithSalad, PrepareSalad
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

    @pytest.mark.skipif(not six.PY2, reason="requires python2")
    def test_notebook(self):
        ipynb = relative_path(salad.__file__, "salad_notebook_py27.ipynb")
        run_test_notebook(ipynb)

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
