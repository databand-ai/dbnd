# © Copyright Databand.ai, an IBM Company 2022

import datetime

from pathlib import Path
from typing import List

from pandas import DataFrame

from dbnd import parameter, task
from dbnd_examples.data import data_repo
from targets import Target
from targets.types import PathStr


class TestDocTaskParameters:
    def test_prepare_data_default(self):
        #### DOC START
        @task
        def prepare_data(data: DataFrame = None) -> DataFrame:
            return data

        #### DOC END
        prepare_data.dbnd_run(data=data_repo.wines)

    def test_prepare_data_parameter_factory(self):
        #### DOC START
        @task
        def prepare_data(data=parameter.default(None)[DataFrame]) -> DataFrame:
            return data

        #### DOC END
        prepare_data.dbnd_run(data=data_repo.wines)

    def test_prepare_data_parameter_decorator(self):
        #### DOC START
        @task(data=parameter.default(None)[DataFrame])
        def prepare_data(data) -> DataFrame:
            return data

        #### DOC END
        prepare_data.dbnd_run(data=data_repo.wines)

    def test_calculate_alpha_value_factory(self):
        #### DOC START
        @task(alpha=parameter.value(0.5))
        def calculate_alpha(alpha) -> float:
            return alpha

        #### DOC END
        calculate_alpha.dbnd_run()

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
        from dbnd import parameter

        @task
        def calculate_alpha(
            alpha=parameter[float],
            debug_level=parameter(significant=False).value(0)[int],
        ):
            return alpha

        #### DOC END
        calculate_alpha.dbnd_run(alpha=0.5)
