# © Copyright Databand.ai, an IBM Company 2022

import pytest
import six

from dbnd_examples_orchestration.data import data_repo, dbnd_examples_data_path
from dbnd_examples_orchestration.orchestration.examples import wine_quality
from dbnd_examples_orchestration.orchestration.examples.wine_quality_as_task_class import (
    PredictWineQuality,
    PredictWineQualityParameterSearch,
)

from dbnd import dbnd_config, relative_path
from dbnd.testing.helpers import run_test_notebook
from dbnd_run.testing.helpers import assert_run_task


class TestWineQualityClasses(object):
    def test_wine_quality_class(self):
        task = PredictWineQualityParameterSearch(alpha_step=0.4)
        assert_run_task(task)

    def test_wine_quality_deco_search(self):
        task = wine_quality.predict_wine_quality_parameter_search.t(
            alpha_step=0.5,
            override={wine_quality.predict_wine_quality.t.data: data_repo.wines},
        )
        assert_run_task(task)

    def test_wine_quality_deco_with_custom_data(self):
        task = wine_quality.predict_wine_quality.t(alpha=0.5, data=data_repo.wines)
        assert_run_task(task)

    @pytest.mark.skipif(not six.PY3, reason="requires python3")
    def test_wine_quality_deco_simple_all(self):
        with dbnd_config(
            {"local_prod": {"_from": "local", "env_label": "prod", "production": True}}
        ):
            task = wine_quality.predict_wine_quality.t(
                alpha=0.5, override={wine_quality.fetch_data.t.task_env: "local_prod"}
            )
            assert_run_task(task)

    def test_wine_quality_deco_simple(self):
        task = wine_quality.predict_wine_quality.t(alpha=0.5)
        assert_run_task(task)

    def test_wine_quality_gz(self):
        task = PredictWineQuality(data=dbnd_examples_data_path("wine_quality.csv.gz"))
        assert_run_task(task)

    def test_prepare_data(self):
        task = wine_quality.prepare_data.t(
            raw_data=dbnd_examples_data_path("wine_quality.csv.gz")
        )
        assert_run_task(task)

    # TODO: Fix this, it works locally in tox but not in CI
    # https://app.asana.com/0/1199880094608584/1200788284410456
    @pytest.mark.skip("doesnt pass in ci")
    def test_notebook(self):
        ipynb = relative_path(wine_quality.__file__, "wine_quality_as_notebook.ipynb")
        run_test_notebook(ipynb)
