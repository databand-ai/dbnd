import pytest
import six

from dbnd import dbnd_config, relative_path
from dbnd.testing.helpers import run_test_notebook
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.data import data_repo, dbnd_examples_data_path
from dbnd_examples.orchestration.examples.wine_quality import (
    wine_quality_decorators,
    wine_quality_decorators_py2,
)
from dbnd_examples.orchestration.examples.wine_quality.wine_quality_classes import (
    PredictWineQuality,
    PredictWineQualityParameterSearch,
)


class TestWineQualityClasses(object):
    def test_wine_quality_class(self):
        task = PredictWineQualityParameterSearch(alpha_step=0.4)
        assert_run_task(task)

    def test_wine_quality_deco_search(self):
        task = wine_quality_decorators.predict_wine_quality_parameter_search.t(
            alpha_step=0.5,
            override={
                wine_quality_decorators.predict_wine_quality.t.data: data_repo.wines
            },
        )
        assert_run_task(task)

    def test_wine_quality_deco_with_custom_data(self):
        task = wine_quality_decorators.predict_wine_quality.t(
            alpha=0.5, data=data_repo.wines
        )
        assert_run_task(task)

    @pytest.mark.skipif(not six.PY3, reason="requires python3")
    def test_wine_quality_deco_simple_all(self):
        with dbnd_config(
            {"local_prod": {"_from": "local", "env_label": "prod", "production": True}}
        ):
            task = wine_quality_decorators.predict_wine_quality.t(
                alpha=0.5,
                override={wine_quality_decorators.fetch_data.t.task_env: "local_prod"},
            )
            assert_run_task(task)

    def test_wine_quality_deco_simple_py2(self):
        task = wine_quality_decorators_py2.predict_wine_quality.t(alpha=0.5)
        assert_run_task(task)

    def test_wine_quality_gz(self):
        task = PredictWineQuality(data=dbnd_examples_data_path("wine_quality.csv.gz"))
        assert_run_task(task)

    def test_prepare_data(self):
        task = wine_quality_decorators.prepare_data.t(
            raw_data=dbnd_examples_data_path("wine_quality.csv.gz")
        )
        assert_run_task(task)

    @pytest.mark.skipif(not six.PY3, reason="requires python3")
    def test_notebook(self):
        ipynb = relative_path(
            wine_quality_decorators.__file__, "predict_wine_quality_py3.ipynb"
        )
        run_test_notebook(ipynb)
