#### TESTED

import datetime
import pathlib

from pathlib import Path
from typing import List

import pandas as pd

from pandas import DataFrame

import targets

from dbnd import PythonTask, parameter, task
from dbnd_examples.data import data_repo
from targets import Target
from targets.types import PathStr


class TestDocTaskParameters:
    def test_prepare_data_simple_types(self):
        #### DOC START
        @task
        def prepare_data(v_int: int, v_bool: bool, v_str: str) -> str:
            pass

        #### DOC END
        prepare_data.dbnd_run(v_int=1, v_bool=True, v_str="hello!")

    def test_prepare_data_datetime_types(self):
        #### DOC START
        @task
        def prepare_data(
            v_datetime: datetime.datetime,
            v_date: datetime.date,
            v_period: datetime.timedelta,
        ) -> str:
            pass

        #### DOC END
        prepare_data.dbnd_run(
            v_datetime=datetime.datetime.now(),
            v_date=datetime.date.today(),
            v_period=datetime.timedelta(days=51),
        )

    def test_prepare_data_path_types(self):
        #### DOC START
        @task
        def prepare_data(
            pathlib_path: Path, str_as_path: PathStr, target_path: Target
        ) -> str:
            assert isinstance(str_as_path, str)
            pass

        #### DOC END

    def test_prepare_data_parameter(self):
        #### DOC START
        class PrepareData(PythonTask):
            data = parameter[DataFrame]
            #### DOC END
            def run(self):
                return self.data

        PrepareData(data=data_repo.wines).dbnd_run()

    def test_calculate_alpha_with_parameter_value(self):
        #### DOC START
        class CalculateAlpha(PythonTask):
            alpha = parameter.value(0.5)

        d = CalculateAlpha(alpha=0.4)
        print(d.alpha)
        #### DOC END

    def test_prepare_data_various_lists(self):
        #### DOC START
        @task
        def prepare_data(
            v_list: List,
            v_list_obj: List[object],
            v_list_str: List[str],
            v_list_int: List[int],
            v_list_date: List[datetime.date],
        ) -> str:
            assert isinstance(v_list_date[0], datetime.date)

            assert isinstance(v_list_obj[0], int)
            assert isinstance(v_list_obj[1], str)

            assert isinstance(v_list_int[0], int)
            assert isinstance(v_list_str[0], str)

            return "OK"

        #### DOC END

    def test_insignificant_parameters(self):
        #### DOC START
        from dbnd import PythonTask, parameter

        class CalculateAlpha(PythonTask):
            alpha = parameter[int]

        class CalculateBeta(CalculateAlpha):
            beta = parameter(significant=False)[int]

        a = CalculateBeta(alpha=0.5, beta=0.1)
        b = CalculateBeta(alpha=0.5, beta=0.2)

        assert a.beta == 0.1
        assert b.beta == 0.1
        assert a is b
        #### DOC END
